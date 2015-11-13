/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import scala.collection.{Seq, Set, mutable}
import kafka.api._
import kafka.cluster.Broker
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.utils.Utils._
import kafka.common.{ErrorMapping, ReplicaNotAvailableException, LeaderNotAvailableException}
import kafka.common.TopicAndPartition
import kafka.controller.KafkaController.StateChangeLogger

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 *  每一个partition缓存的状态对象
 *  该缓存状态信息通过controller的UpdateMetadataRequest请求更新
 *  每一个节点保持一样的缓存,更新是异步的
 *  
 *  
 *  缓存
 *  1.topic-partition-PartitionStateInfo映射关系
 *  2.缓存活着的broker节点信息
 */
private[server] class MetadataCache {
  //key是topic,value是Map,key是partitionId,value是PartitionStateInfo
  private val cache: mutable.Map[String, mutable.Map[Int, PartitionStateInfo]] =
    new mutable.HashMap[String, mutable.Map[Int, PartitionStateInfo]]()
  private var aliveBrokers: Map[Int, Broker] = Map() //活着的节点集合
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  //返回值 ListBuffer[TopicMetadata],根据参数topic集合,返回该topic的信息内容
  //如果参数为空,则表示返回所有的topic信息
  def getTopicMetadata(topics: Set[String]) = {
    val isAllTopics = topics.isEmpty//是否参数集合为空
    val topicsRequested = if(isAllTopics) cache.keySet else topics//寻找本次请求的topic集合
    val topicResponses: mutable.ListBuffer[TopicMetadata] = new mutable.ListBuffer[TopicMetadata] //返回值
    
    inReadLock(partitionMetadataLock) {
      for (topic <- topicsRequested) {//循环每一个topic
        if (isAllTopics || cache.contains(topic)) {
          val partitionStateInfos = cache(topic) //返回值 Map[Int, PartitionStateInfo]
          val partitionMetadata = partitionStateInfos.map {
            case (partitionId, partitionState) =>
              val replicas = partitionState.allReplicas
              val replicaInfo: Seq[Broker] = replicas.map(aliveBrokers.getOrElse(_, null)).filter(_ != null).toSeq
              var leaderInfo: Option[Broker] = None
              var isrInfo: Seq[Broker] = Nil
              val leaderIsrAndEpoch = partitionState.leaderIsrAndControllerEpoch
              val leader = leaderIsrAndEpoch.leaderAndIsr.leader
              val isr = leaderIsrAndEpoch.leaderAndIsr.isr
              val topicPartition = TopicAndPartition(topic, partitionId)
              try {
                leaderInfo = aliveBrokers.get(leader)
                if (!leaderInfo.isDefined)
                  throw new LeaderNotAvailableException("Leader not available for %s".format(topicPartition))
                isrInfo = isr.map(aliveBrokers.getOrElse(_, null)).filter(_ != null)
                if (replicaInfo.size < replicas.size)
                  throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                    replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
                if (isrInfo.size < isr.size)
                  throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                    isr.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
                new PartitionMetadata(partitionId, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
              } catch {
                case e: Throwable =>
                  debug("Error while fetching metadata for %s. Possible cause: %s".format(topicPartition, e.getMessage))
                  new PartitionMetadata(partitionId, leaderInfo, replicaInfo, isrInfo,
                    ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
              }
          }
          topicResponses += new TopicMetadata(topic, partitionMetadata.toSeq)
        }
      }
    }
    topicResponses
  }

  //获取活着的节点Broker集合
  def getAliveBrokers = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toSeq
    }
  }

  //添加topic-partition-PartitionStateInfo映射关系
  def addOrUpdatePartitionInfo(topic: String,
                               partitionId: Int,
                               stateInfo: PartitionStateInfo) {
    inWriteLock(partitionMetadataLock) {
      cache.get(topic) match {
        case Some(infos) => infos.put(partitionId, stateInfo)
        case None => {
          val newInfos: mutable.Map[Int, PartitionStateInfo] = new mutable.HashMap[Int, PartitionStateInfo]
          cache.put(topic, newInfos)
          newInfos.put(partitionId, stateInfo)
        }
      }
    }
  }

  //根据topic-partition返回缓存的PartitionStateInfo对象
  def getPartitionInfo(topic: String, partitionId: Int): Option[PartitionStateInfo] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic) match {
        case Some(partitionInfos) => partitionInfos.get(partitionId)
        case None => None
      }
    }
  }

  //更新缓存
  def updateCache(updateMetadataRequest: UpdateMetadataRequest,
                  brokerId: Int,
                  stateChangeLogger: StateChangeLogger) {
    inWriteLock(partitionMetadataLock) {
      //更新最新的活的节点信息
      aliveBrokers = updateMetadataRequest.aliveBrokers.map(b => (b.id, b)).toMap
      
      //更新topic-partition-PartitionStateInfo映射关系
      updateMetadataRequest.partitionStateInfos.foreach { case(tp, info) =>
        if (info.leaderIsrAndControllerEpoch.leaderAndIsr.leader == LeaderAndIsr.LeaderDuringDelete) {//表示该leader正在删除中
          removePartitionInfo(tp.topic, tp.partition) //移除该映射
          stateChangeLogger.trace(("Broker %d deleted partition %s from metadata cache in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, tp, updateMetadataRequest.controllerId,
            updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId))
        } else {
          //添加三者的映射关系topic-partition-PartitionStateInfo
          addOrUpdatePartitionInfo(tp.topic, tp.partition, info)
          //打印日志
          stateChangeLogger.trace(("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, info, tp, updateMetadataRequest.controllerId,
            updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId))
        }
      }
    }
  }

  //移除topic-partition映射关系
  private def removePartitionInfo(topic: String, partitionId: Int) = {
    cache.get(topic) match {
      case Some(infos) => {
        infos.remove(partitionId)
        if(infos.isEmpty) {
          cache.remove(topic)
        }
        true
      }
      case None => false
    }
  }
}

