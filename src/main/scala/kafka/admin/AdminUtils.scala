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

package kafka.admin

import java.util.Random
import java.util.Properties
import kafka.api.{TopicMetadata, PartitionMetadata}
import kafka.cluster.Broker
import kafka.log.LogConfig
import kafka.utils.{Logging, ZkUtils, Json}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import scala.collection._
import mutable.ListBuffer
import scala.collection.mutable
import kafka.common._
import scala.Predef._
import collection.Map
import scala.Some
import collection.Set

object AdminUtils extends Logging {
  val rand = new Random
  val TopicConfigChangeZnodePrefix = "config_change_"

  /**
   * There are 2 goals of replica assignment:这有两个重新分配备份节点的目标
   * 1. Spread the replicas evenly among brokers.保证在节点间均匀传播备份数据
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.将partition的备份分配到指定的节点上
   *
   * To achieve this goal, we:为了完成这个目标,我们做了以下的事儿
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 每一个partition的第一个备份通过轮训方式被分配到节点上,开始位置从节点集合中随机产生
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   * 每一个partition剩下的备份是通过增长的方式被分配
   *
   * Here is an example of assigning
   * 例如有5个节点
   *
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   * 重方法的意义 新为topic-partition分配备份节点
   * 返回值就是每一个partition分配到哪些节点上了Map[Int, Seq[Int]]
   */
  def assignReplicasToBrokers(brokerList: Seq[Int],//目标移动到这些节点上去备份
                              nPartitions: Int,//目前该topic有多少个partition
                              replicationFactor: Int,//目前该topic的第一个partition有多少个备份
                              fixedStartIndex: Int = -1,//固定开始位置
                              startPartitionId: Int = -1)//从第几个partition开始分配
  : Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size)//不允许备份数量>节点数量,导致一个节点分配多个备份是不允许的

    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)//随机产生一个开始位置还是固定开始位置
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0//设置当前位置

    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size) //从固定或者随机产生一个备份节点
    for (i <- 0 until nPartitions) {//循环每一个partition
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))//已经确定了第一个备份节点
      for (j <- 0 until replicationFactor - 1)//确定剩余的备份节点
        replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    ret.toMap
  }


 /**
  * Add partitions to existing topic with optional replica assignment
  *
  * @param zkClient Zookeeper client
  * @param topic Topic for adding partitions to
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignmentStr Manual replica assignment
  * @param checkBrokerAvailable Ignore checking if assigned replica broker is available. Only used for testing
  * @param config Pre-existing properties that should be preserved
  * 为topic添加partition
  */
  def addPartitions(zkClient: ZkClient,
                    topic: String,//topic名称
                    numPartitions: Int = 1,//partiton数量,默认是1个
                    replicaAssignmentStr: String = "",//手动为partition添加备份到哪些节点上
                    checkBrokerAvailable: Boolean = true,
                    config: Properties = new Properties) {
    val existingPartitionsReplicaList = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic)) //返回值 Map[TopicAndPartition, Seq[Int]] key是topic-partition,value是该partition都在哪些节点有备份
    if (existingPartitionsReplicaList.size == 0)
      throw new AdminOperationException("The topic %s does not exist".format(topic))//topic不存在是不允许的

    val existingReplicaList = existingPartitionsReplicaList.head._2//获取第一个topic-partition对应的节点备份集合Seq[Int]
    val partitionsToAdd = numPartitions - existingPartitionsReplicaList.size//返回要添加多少个partition
    if (partitionsToAdd <= 0)
      throw new AdminOperationException("The number of partitions for a topic can only be increased")//topic-partition的数量不允许<=0

    // create the new partition replication list
    val brokerList = ZkUtils.getSortedBrokerList(zkClient) //获取所有的节点集合

   //返回值就是每一个partition分配到哪些节点上了Map[Int, Seq[Int]]
    val newPartitionReplicaList = if (replicaAssignmentStr == null || replicaAssignmentStr == "")
     /**
      * 参数集合
      * 在所有的节点上
      * 分配partitionsToAdd个partition
      * 每一个partition需要existingReplicaList.size个备份
      * 第一个partition的第一个备份在哪个节点上
      * 现在已经存在的每一个topic-partition的备份的个数
      *
      * 返回值就是每一个partition分配到哪些节点上了Map[Int, Seq[Int]]
      */
      AdminUtils.assignReplicasToBrokers(brokerList, partitionsToAdd, existingReplicaList.size, existingReplicaList.head, existingPartitionsReplicaList.size)
    else
   //手动分配
      getManualReplicaAssignment(replicaAssignmentStr, brokerList.toSet, existingPartitionsReplicaList.size, checkBrokerAvailable)

    // check if manual assignment has the right replication factor 分配有问题,没有达到备份数量
    val unmatchedRepFactorList = newPartitionReplicaList.values.filter(p => (p.size != existingReplicaList.size))
    if (unmatchedRepFactorList.size != 0)
      throw new AdminOperationException("The replication factor in manual replication assignment " +
        " is not equal to the existing replication factor for the topic " + existingReplicaList.size)

   //合并新老的partition和对应的备份节点集合
    info("Add partition list for %s is %s".format(topic, newPartitionReplicaList))
    val partitionReplicaList = existingPartitionsReplicaList.map(p => p._1.partition -> p._2)
    // add the new list
    partitionReplicaList ++= newPartitionReplicaList

   //合并后的写入到zookeeper中
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaList, config, true)
  }

  //手动为partition添加备份到哪些节点上
  def getManualReplicaAssignment(replicaAssignmentList: String, availableBrokerList: Set[Int], startPartitionId: Int, checkBrokerAvailable: Boolean = true): Map[Int, List[Int]] = {
    var partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    var partitionId = startPartitionId
    partitionList = partitionList.takeRight(partitionList.size - partitionId)
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      if (brokerList.size <= 0)
        throw new AdminOperationException("replication factor must be larger than 0")
      if (brokerList.size != brokerList.toSet.size)
        throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList)
      if (checkBrokerAvailable && !brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString +
          "available broker:" + availableBrokerList.toString)
      ret.put(partitionId, brokerList.toList)
      if (ret(partitionId).size != ret(startPartitionId).size)
        throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList)
      partitionId = partitionId + 1
    }
    ret.toMap
  }
  
  def deleteTopic(zkClient: ZkClient, topic: String) {
    ZkUtils.createPersistentPath(zkClient, ZkUtils.getDeleteTopicPath(topic))
  }
  
  def topicExists(zkClient: ZkClient, topic: String): Boolean = 
    zkClient.exists(ZkUtils.getTopicPath(topic))

  /**
   * 创建一个topic
   * @param zkClient
   * @param topic topic名称
   * @param partitions 设置该topic有多少个partition
   * @param replicationFactor 设置topic的每一个partition要保留多少个备份
   * @param topicConfig 设置额外的配置信息
   */
  def createTopic(zkClient: ZkClient,
                  topic: String,
                  partitions: Int, 
                  replicationFactor: Int, 
                  topicConfig: Properties = new Properties) {
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, topicConfig)
  }

  /**
   * 创建或者更新每一个topic对应的partition在哪些节点上去备份,并且写入到zookeeper中
   */
  def createOrUpdateTopicPartitionAssignmentPathInZK(zkClient: ZkClient,
                                                     topic: String,//topic
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],//每一个partition对应在哪个节点上有备份
                                                     config: Properties = new Properties,//topic的配置信息
                                                     update: Boolean = false) {//true表示更新
    // validate arguments
    Topic.validate(topic)
    LogConfig.validate(config)
    require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.")//校验所有的partition应该有相同的备份数量

    val topicPath = ZkUtils.getTopicPath(topic) ///brokers/topics/${topic}
    if(!update && zkClient.exists(topicPath))
      throw new TopicExistsException("Topic \"%s\" already exists.".format(topic))
    partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))//确保每一个partition分配的备份节点集合中不允许有重复的节点ID
    
    // write out the config if there is any, this isn't transactional with the partition assignments
    //将topic的配置信息写入到zookeeper中
    writeTopicConfig(zkClient, topic, config)
    
    // create the partition assignment
    writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update)
  }

  /**
   * 创建或者更新每一个topic对应的partition在哪些节点上去备份,并且写入到zookeeper中
   * @param replicaAssignment 每一个partition对应在哪个节点上有备份
   * @param update true表示更新
   */
  private def writeTopicPartitionAssignment(zkClient: ZkClient, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic) ///brokers/topics/${topic}
      val jsonPartitionData = ZkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)))

      if (!update) {//创建
        info("Topic creation " + jsonPartitionData.toString)
        ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData)
      } else {//更新
        info("Topic update " + jsonPartitionData.toString)
        ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData)
      }
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }
  
  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   * @param zkClient: The ZkClient handle used to write the new config to zookeeper
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *  将topic的配置信息写入到zookeeper下
   */
  def changeTopicConfig(zkClient: ZkClient, topic: String, configs: Properties) {
    if(!topicExists(zkClient, topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))

    // remove the topic overrides
    LogConfig.validate(configs)

    // write the new config--may not exist if there were previously no overrides将配置信息写入到zookeeper中
    writeTopicConfig(zkClient, topic, configs)
    
    // create the change notification
    //创建更改配置信息的zookeeper节点 /config/changes/config_change_序号,内容是topic名称
    zkClient.createPersistentSequential(ZkUtils.TopicConfigChangesPath + "/" + TopicConfigChangeZnodePrefix, Json.encode(topic))
  }
  
  /**
   * Write out the topic config to zk, if there is any
   * 写入一个topic的配置信息到zookeeper中
   */
  private def writeTopicConfig(zkClient: ZkClient, topic: String, config: Properties) {
    val configMap: mutable.Map[String, String] = {
      import JavaConversions._
      config
    }

    //对/config/topics/${topic}节点内容进行更新
    val map = Map("version" -> 1, "config" -> configMap)
    ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicConfigPath(topic), Json.encode(map))
  }
  
  /**
   * Read the topic config (if any) from zk
   * 读取topic的配置信息集合
   * 内容格式:{version:1,config:{key=value,key=value}}
   */
  def fetchTopicConfig(zkClient: ZkClient, topic: String): Properties = {
    val str: String = zkClient.readData(ZkUtils.getTopicConfigPath(topic), true)//获取该topic的配置信息
    val props = new Properties()
    if(str != null) {
      Json.parseFull(str) match {
        case None => // there are no config overrides 没有数据
        case Some(map: Map[String, _]) => 
          require(map("version") == 1)
          map.get("config") match {
            case Some(config: Map[String, String]) =>
              for((k,v) <- config)
                props.setProperty(k, v)
            case _ => throw new IllegalArgumentException("Invalid topic config: " + str)
          }

        case o => throw new IllegalArgumentException("Unexpected value in config: "  + str)
      }
    }
    props
  }

  //返回map,key是topic的name,value是该topic的独特的配置信息
  def fetchAllTopicConfigs(zkClient: ZkClient): Map[String, Properties] =
    ZkUtils.getAllTopics(zkClient).map(topic => (topic, fetchTopicConfig(zkClient, topic))).toMap

  def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient): TopicMetadata =
    fetchTopicMetadataFromZk(topic, zkClient, new mutable.HashMap[Int, Broker])

  def fetchTopicMetadataFromZk(topics: Set[String], zkClient: ZkClient): Set[TopicMetadata] = {
    val cachedBrokerInfo = new mutable.HashMap[Int, Broker]()
    topics.map(topic => fetchTopicMetadataFromZk(topic, zkClient, cachedBrokerInfo))
  }

  //抓去topic的所有元数据信息
  private def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient, cachedBrokerInfo: mutable.HashMap[Int, Broker]): TopicMetadata = {
    if(ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic))) {//首先topic必须存在
      val topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic).get //返回key是topic,value的key是该topic的partition,value是该partition对应的brokerId集合
      val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)//对partition集合进行排序,collection.Map[Int, Seq[Int] 返回的key 是partition,value是该partition所在节点集合
      val partitionMetadata = sortedPartitions.map { partitionMap =>
        val partition = partitionMap._1//partition序号
        val replicas = partitionMap._2//备份节点集合
        val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partition)//获取该partition的同步集合
        val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)//获取该partition的leader节点
        debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader)

        var leaderInfo: Option[Broker] = None
        var replicaInfo: Seq[Broker] = Nil
        var isrInfo: Seq[Broker] = Nil
        try {
          leaderInfo = leader match {
            case Some(l) =>
              try {
                Some(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, List(l)).head)//获取leader所在的节点对象
              } catch {
                case e: Throwable => throw new LeaderNotAvailableException("Leader not available for partition [%s,%d]".format(topic, partition), e)//该partition不可用
              }
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)//该partition没有leader节点
          }
          try {
            replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas.map(id => id.toInt))//获取备份节点对象集合
            isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas)//获取同步的节点对象集合
          } catch {
            case e: Throwable => throw new ReplicaNotAvailableException(e)
          }
          if(replicaInfo.size < replicas.size)
            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
              replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
          if(isrInfo.size < inSyncReplicas.size)
            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
              inSyncReplicas.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
          new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
        } catch {
          case e: Throwable =>
            debug("Error while fetching metadata for partition [%s,%d]".format(topic, partition), e)
            new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo,
              ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
        }
      }
      new TopicMetadata(topic, partitionMetadata)
    } else {
      // topic doesn't exist, send appropriate error code
      new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
    }
  }

  //对已经获取的节点信息进行缓存,提高效率
  private def getBrokerInfoFromCache(zkClient: ZkClient,
                                     cachedBrokerInfo: scala.collection.mutable.Map[Int, Broker],//缓存的节点信息,节点ID对应节点对象
                                     brokerIds: Seq[Int])//查找这些节点ID对应的Broker对象,可以优先从缓存中查找
                                   : Seq[Broker] = {
    var failedBrokerIds: ListBuffer[Int] = new ListBuffer()//加载失败的节点ID
    val brokerMetadata = brokerIds.map { id =>
      val optionalBrokerInfo = cachedBrokerInfo.get(id)//是否缓存名字该节点ID
      optionalBrokerInfo match {
        case Some(brokerInfo) => Some(brokerInfo) // return broker info from the cache 命中,则从缓存中获取该节点ID对应的节点
        case None => // fetch it from zookeeper
          ZkUtils.getBrokerInfo(zkClient, id) match {//从zookeeper中获取该节点对象,并且加入到缓存中
            case Some(brokerInfo) =>
              cachedBrokerInfo += (id -> brokerInfo)
              Some(brokerInfo)
            case None =>
              failedBrokerIds += id
              None
          }
      }
    }
    brokerMetadata.filter(_.isDefined).map(_.get)
  }

  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}
