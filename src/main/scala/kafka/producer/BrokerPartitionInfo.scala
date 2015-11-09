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
package kafka.producer

import collection.mutable.HashMap
import kafka.api.TopicMetadata
import kafka.common.KafkaException
import kafka.utils.Logging
import kafka.common.ErrorMapping
import kafka.client.ClientUtils


/**
 * topicPartitionInfo: HashMap[String, TopicMetadata] 参数中key是topic字符串,value是该topic下所有partition的元数据信息
 * 
 * 该类缓存每一个topic的元数据信息
 */
class BrokerPartitionInfo(producerConfig: ProducerConfig,
                          producerPool: ProducerPool,
                          topicPartitionInfo: HashMap[String, TopicMetadata])
        extends Logging {
  val brokerList = producerConfig.brokerList
  val brokers = ClientUtils.parseBrokerList(brokerList)

  /**
   * Return a sequence of (brokerId, numPartitions).
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions). Returns a zero-length
   * sequence if no brokers are available.
   * 获取该topic对应的元数据集合信息
   * 并且元数据集合信息是PartitionAndLeader集合形式返回,并且按照partitionId排序了的集合
   */
  def getBrokerPartitionInfo(topic: String, correlationId: Int): Seq[PartitionAndLeader] = {
    debug("Getting broker partition info for topic %s".format(topic))
    // check if the cache has metadata for this topic 检查缓存中是否已经存在了该topic的元数据信息
    val topicMetadata = topicPartitionInfo.get(topic)
    val metadata: TopicMetadata =
      topicMetadata match {
        case Some(m) => m//缓存中命中,则返回缓存数据
        case None =>
          // refresh the topic metadata cache
          updateInfo(Set(topic), correlationId)//更新该topic对应元数据信息
          val topicMetadata = topicPartitionInfo.get(topic)//更新后,再从缓存中查询,如果查询到则返回,查询不到则抛异常
          topicMetadata match {
            case Some(m) => m
            case None => throw new KafkaException("Failed to fetch topic metadata for topic: " + topic)//抛异常,说明该topic无法抓取元数据
          }
      }
    //获取该topic对应的PartitionMetadata集合
    val partitionMetadata = metadata.partitionsMetadata
    if(partitionMetadata.size == 0) {
      if(metadata.errorCode != ErrorMapping.NoError) {//出现异常则抛出
        throw new KafkaException(ErrorMapping.exceptionFor(metadata.errorCode))
      } else {//说明topic没有对应的partition元数据信息,但是却没有返回异常状态码
        throw new KafkaException("Topic metadata %s has empty partition metadata and no error code".format(metadata))
      }
    }
    
    //循环该topic对应的PartitionMetadata元数据集合,并且按照partitionId排序
    partitionMetadata.map { m =>
      m.leader match {
        case Some(leader) =>
          debug("Partition [%s,%d] has leader %d".format(topic, m.partitionId, leader.id))
          new PartitionAndLeader(topic, m.partitionId, Some(leader.id))
        case None =>
          debug("Partition [%s,%d] does not have a leader yet".format(topic, m.partitionId))
          new PartitionAndLeader(topic, m.partitionId, None)
      }
    }.sortWith((s, t) => s.partitionId < t.partitionId)
  }

  /**
   * It updates the cache by issuing a get topic metadata request to a random broker.
   * @param topics the topics for which the metadata is to be fetched
   * 更新topic集合中所有topic的元数据
   */
  def updateInfo(topics: Set[String], correlationId: Int) {
    var topicsMetadata: Seq[TopicMetadata] = Nil
    //抓取brokers节点集合上 topics集合的元数据休息
    val topicMetadataResponse = ClientUtils.fetchTopicMetadata(topics, brokers, producerConfig, correlationId)
    //请求的返回值
    topicsMetadata = topicMetadataResponse.topicsMetadata
    // throw partition specific exception
    topicsMetadata.foreach(tmd =>{
      trace("Metadata for topic %s is %s".format(tmd.topic, tmd))
      if(tmd.errorCode == ErrorMapping.NoError) {//如果返回的topic元数据没有异常,则添加到缓存集合中
        topicPartitionInfo.put(tmd.topic, tmd)
      } else
        warn("Error while fetching metadata [%s] for topic [%s]: %s ".format(tmd, tmd.topic, ErrorMapping.exceptionFor(tmd.errorCode).getClass))
      tmd.partitionsMetadata.foreach(pmd =>{
        if (pmd.errorCode != ErrorMapping.NoError && pmd.errorCode == ErrorMapping.LeaderNotAvailableCode) {
          warn("Error while fetching metadata %s for topic partition [%s,%d]: [%s]".format(pmd, tmd.topic, pmd.partitionId,
            ErrorMapping.exceptionFor(pmd.errorCode).getClass))
        } // any other error code (e.g. ReplicaNotAvailable) can be ignored since the producer does not need to access the replica and isr metadata
      })
    })
    producerPool.updateProducer(topicsMetadata)
  }
  
}

//描述每一个topic-partition对应的是否存在leader partition
case class PartitionAndLeader(topic: String, partitionId: Int, leaderBrokerIdOpt: Option[Int])
