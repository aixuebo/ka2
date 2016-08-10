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

import kafka.api._
import kafka.common._
import kafka.utils._
import kafka.cluster.{Broker, Partition, Replica}
import kafka.log.LogManager
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.common.TopicAndPartition
import kafka.message.MessageSet

import java.util.concurrent.atomic.AtomicBoolean
import java.io.{IOException, File}
import java.util.concurrent.TimeUnit
import scala.Predef._
import scala.collection._
import scala.collection.mutable.HashMap
import scala.collection.Map
import scala.collection.Set
import scala.Some

import org.I0Itec.zkclient.ZkClient
import com.yammer.metrics.core.Gauge

//kafka还可以配置partitions需要备份的个数(replicas),每个partition将会被备份到多台机器上,以提高可用性.
object ReplicaManager {
  //该文件存储的值表示该partition所有的同步节点集合至少也同步到什么位置了
  val HighWatermarkFilename = "replication-offset-checkpoint"
}

/**
 *
 * @param data follower 节点抓到的partition的数据内容
 * @param offset 从什么序号开始抓去的数据
 */
case class PartitionDataAndOffset(data: FetchResponsePartitionData, offset: LogOffsetMetadata)


class ReplicaManager(val config: KafkaConfig, 
                     time: Time,
                     val zkClient: ZkClient,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean ) extends Logging with KafkaMetricsGroup {
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1 /* epoch of the controller that last changed the leader */
  private val localBrokerId = config.brokerId//本地日志属于哪个节点的ID
  //key是topic-partition组成,value是组成的Partition对象,该对象管理这副本备份信息
  private val allPartitions = new Pool[(String, Int), Partition]//就是一个Map,通过(String, Int)参数可以返回一个Partition对象
  private val replicaStateChangeLock = new Object
  
  //从leader节点抓取数据,并且将数据抓取成功后添加到对应的log文件中
  val replicaFetcherManager = new ReplicaFetcherManager(config, this)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)

  //在每一个log磁盘下,创建一个OffsetCheckpoint对象,文件名是replication-offset-checkpoint
  //key是log磁盘根目录,value是replication-offset-checkpoint文件
  //该文件存储的值表示该partition所有的同步节点集合至少也同步到什么位置了
  val highWatermarkCheckpoints = config.logDirs.map(dir => (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap

  private var hwThreadInitialized = false //true表示已经打开了一个线程,定期向replication-offset-checkpoint文件写入数据
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = KafkaController.stateChangeLogger

  var producerRequestPurgatory: ProducerRequestPurgatory = null
  var fetchRequestPurgatory: FetchRequestPurgatory = null

  newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = {
          getLeaderPartitions().size
      }
    }
  )
  newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = underReplicatedPartitionCount()
    }
  )
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS) //扩展同步节点集合
  val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS) //缩小同步节点集合

  def underReplicatedPartitionCount(): Int = {
      getLeaderPartitions().count(_.isUnderReplicated)
  }

  //打开线程,定期保存每一个partition的记录
  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  /**
   * Initialize the replica manager with the request purgatory
   *
   * TODO: will be removed in 0.9 where we refactor server structure
   */

  def initWithRequestPurgatory(producerRequestPurgatory: ProducerRequestPurgatory, fetchRequestPurgatory: FetchRequestPurgatory) {
    this.producerRequestPurgatory = producerRequestPurgatory
    this.fetchRequestPurgatory = fetchRequestPurgatory
  }

  /**
   * Unblock some delayed produce requests with the request key
   */
  def unblockDelayedProduceRequests(key: TopicAndPartition) {
    val satisfied = producerRequestPurgatory.update(key)
    debug("Request key %s unblocked %d producer requests."
      .format(key, satisfied.size))

    // send any newly unblocked responses
    satisfied.foreach(producerRequestPurgatory.respond(_))
  }

  /**
   * Unblock some delayed fetch requests with the request key
   */
  def unblockDelayedFetchRequests(key: TopicAndPartition) {
    val satisfied = fetchRequestPurgatory.update(key)
    debug("Request key %s unblocked %d fetch requests.".format(key, satisfied.size))

    // send any newly unblocked responses
    satisfied.foreach(fetchRequestPurgatory.respond(_))
  }

  //入口函数
  def startup() {
    // start ISR expiration thread
    scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS)
  }

  /**
   * 该节点不再存储该topic-partition备份信息
   * @param deletePartition true表示要删除该partition日志
   */
  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace("Broker %d handling stop replica (delete=%s) for partition [%s,%d]".format(localBrokerId,
      deletePartition.toString, topic, partitionId))
    val errorCode = ErrorMapping.NoError
    getPartition(topic, partitionId) match {
      case Some(partition) =>
        if(deletePartition) {
          val removedPartition = allPartitions.remove((topic, partitionId))
          if (removedPartition != null)
            removedPartition.delete() // this will delete the local log
        }
      case None =>
        //闪入日志和相应的文件夹
        // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
        // This could happen when topic is being deleted while broker is down and recovers.
        if(deletePartition) {
          val topicAndPartition = TopicAndPartition(topic, partitionId)

          if(logManager.getLog(topicAndPartition).isDefined) {
              logManager.deleteLog(topicAndPartition)
          }
        }
        stateChangeLogger.trace("Broker %d ignoring stop replica (delete=%s) for partition [%s,%d] as replica doesn't exist on broker"
          .format(localBrokerId, deletePartition, topic, partitionId))
    }
    stateChangeLogger.trace("Broker %d finished handling stop replica (delete=%s) for partition [%s,%d]"
      .format(localBrokerId, deletePartition, topic, partitionId))
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicAndPartition, Short], Short) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicAndPartition, Short]
      if(stopReplicaRequest.controllerEpoch < controllerEpoch) {//请求枚举次数必须校验合法
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d."
          .format(localBrokerId, stopReplicaRequest.controllerEpoch) +
          " Latest known controller epoch is %d " + controllerEpoch)
        (responseMap, ErrorMapping.StaleControllerEpochCode)
      } else {
        controllerEpoch = stopReplicaRequest.controllerEpoch //更新枚举校验值
        // First stop fetchers for all partitions, then stop the corresponding replicas
        //首先停止抓去这些partition
        replicaFetcherManager.removeFetcherForPartitions(stopReplicaRequest.partitions.map(r => TopicAndPartition(r.topic, r.partition)))
        for(topicAndPartition <- stopReplicaRequest.partitions){//以此删除这些partition
          val errorCode = stopReplica(topicAndPartition.topic, topicAndPartition.partition, stopReplicaRequest.deletePartitions)
          responseMap.put(topicAndPartition, errorCode)
        }
        (responseMap, ErrorMapping.NoError)
      }
    }
  }

  //通过topic-partitionId获取Partition对象,如果不存在,则创建一个对应的Partition对象
  def getOrCreatePartition(topic: String, partitionId: Int): Partition = {
    var partition = allPartitions.get((topic, partitionId))
    if (partition == null) {
      allPartitions.putIfNotExists((topic, partitionId), new Partition(topic, partitionId, time, this))
      partition = allPartitions.get((topic, partitionId))
    }
    partition
  }

  //通过topic-partitionId获取Partition对象
  def getPartition(topic: String, partitionId: Int): Option[Partition] = {
    val partition = allPartitions.get((topic, partitionId))
    if (partition == null)
      None
    else
      Some(partition)
  }

  //通过topic-partitionId获取Partition对象,从而获取replicaId节点备份对象Replica
  //如果获取不到备份对象Replica,则抛异常
  def getReplicaOrException(topic: String, partition: Int): Replica = {
    val replicaOpt = getReplica(topic, partition)
    if(replicaOpt.isDefined)
      return replicaOpt.get
    else{
      //表示该topic-partiton对应的本机Replica备份对象,是不可用的
      throw new ReplicaNotAvailableException("Replica %d is not available for partition [%s,%d]".format(config.brokerId, topic, partition))
    }
  }

  //获取leader,并且本地的就是leader
  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException("Partition [%s,%d] doesn't exist on %d".format(topic, partitionId, config.brokerId))
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
                                                     .format(topic, partitionId, config.brokerId))
        }
    }
  }

  //通过topic-partitionId获取Partition对象,从而获取replicaId节点的备份对象Replica
  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  /**
   * Read from all the offset details given and return a map of
   * (topic, partition) -> PartitionData
   * 输入参数中读取Map fetchRequest.requestInfo.map,内容是key表示抓取哪个topic-partition数据,value表示从offset开始抓取,抓取多少个数据返回
   * 输出Map的key是抓取哪个topic-partition数据,value是该partition抓去到的数据信息
   */
  def readMessageSets(fetchRequest: FetchRequest) = {
    val isFetchFromFollower = fetchRequest.isFromFollower//true表示该请求来自于follower节点
    //fetchRequest.requestInfo发送本次抓取请求,是抓取那些topic-partition,从第几个序号开始抓取,最多抓取多少个字节
    fetchRequest.requestInfo.map{
      case (TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)) =>//抓去哪个topic-partition上从什么offset开始抓去.抓去多少条数据
      val partitionDataAndOffsetInfo =
          try {
            //真正去读取本地文件,返回抓去的数据
            val (fetchInfo, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, fetchRequest.replicaId)//fetchRequest.replicaId哪个follower节点发过来的抓去请求
            //写入统计信息
            BrokerTopicStats.getBrokerTopicStats(topic).bytesOutRate.mark(fetchInfo.messageSet.sizeInBytes)
            BrokerTopicStats.getBrokerAllTopicsStats.bytesOutRate.mark(fetchInfo.messageSet.sizeInBytes)
            if (isFetchFromFollower) {//如果来自与follower节点
              debug("Partition [%s,%d] received fetch request from follower %d"
                .format(topic, partition, fetchRequest.replicaId))//打印日志topic-partition收到了来自哪个follower节点的抓去请求,并且已经在本地文件获取到了文件内容
            }
            new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, fetchInfo.messageSet), fetchInfo.fetchOffset)
          } catch {
            // NOTE: Failed fetch requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
            // since failed fetch requests metric is supposed to indicate failure of a broker in handling a fetch request
            // for a partition it is the leader for
            case utpe: UnknownTopicOrPartitionException =>
              warn("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s".format(
                fetchRequest.correlationId, fetchRequest.clientId, topic, partition, utpe.getMessage))
              new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata)
            case nle: NotLeaderForPartitionException =>
              warn("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s".format(
                fetchRequest.correlationId, fetchRequest.clientId, topic, partition, nle.getMessage))
              new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata)
            case t: Throwable =>
              BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark()
              BrokerTopicStats.getBrokerAllTopicsStats.failedFetchRequestRate.mark()
              error("Error when processing fetch request for partition [%s,%d] offset %d from %s with correlation id %d. Possible cause: %s"
                .format(topic, partition, offset, if (isFetchFromFollower) "follower" else "consumer", fetchRequest.correlationId, t.getMessage))
              new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(t.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata)
          }
        (TopicAndPartition(topic, partition), partitionDataAndOffsetInfo)
    }
  }

  /**
   * Read from a single topic/partition at the given offset upto maxSize bytes
   * 从单独一个topic/partition本地文件的leader中读取数据,从给定offset开始读取,最多读取maxSize个
   * @fromReplicaId 表示该请求是哪个follower节点发过来的
   * 
   * 返回FetchDataInfo, Long
   *
   */
  private def readMessageSet(topic: String,//去读取该topic-partition数据
                             partition: Int,
                             offset: Long,//从offset位置开始读取
                             maxSize: Int,//最多读取多少个字节
                             fromReplicaId: Int) //请求来源于哪个follow节点
                            : (FetchDataInfo, Long) = {
    // check if the current broker is the leader for the partitions
    //因为该方法是follower节点向leader节点调用的,因此该方法一定是在leader的节点上执行的,因此可以获取到leader节点对应的备份对象,即一定是存在本地log文件
    //获取该topic-partition对应的本地文件
    val localReplica = if(fromReplicaId == Request.DebuggingConsumerId)
      getReplicaOrException(topic, partition)
    else
      getLeaderReplicaIfLocal(topic, partition)
    trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize)) //打印日志,开始抓去信息
    val maxOffsetOpt =
      if (Request.isValidBrokerId(fromReplicaId))//说明是follow节点,则不设置最大偏移量
        None
      else
        Some(localReplica.highWatermark.messageOffset)
    val fetchInfo = localReplica.log match {//真正去读取文件记录,然后返回
      case Some(log) =>
        log.read(offset, maxSize, maxOffsetOpt)
      case None =>
        error("Leader for partition [%s,%d] does not have a local log".format(topic, partition))
        FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty)
    }
    (fetchInfo, localReplica.highWatermark.messageOffset)
    //返回值localReplica.highWatermark.messageOffset要告诉每一个客户端follower节点,当前leader已经同步到哪些位置了,即所有的同步集合都至少也拿到了该位置的数据了。该位置之前的数据都已经全部同步完成
  }

  //处理更新元数据请求
  def maybeUpdateMetadataCache(updateMetadataRequest: UpdateMetadataRequest, metadataCache: MetadataCache) {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {//校验controller选举次数异常
        val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
          "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
          updateMetadataRequest.correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
          controllerEpoch)
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateControllerEpochErrorMessage)
      } else {
        metadataCache.updateCache(updateMetadataRequest, localBrokerId, stateChangeLogger)
        controllerEpoch = updateMetadataRequest.controllerEpoch
      }
    }
  }

  //使该节点作为partition的leader,或者作为follow节点
  /**
   * 该方法是入口方法
   * 该方法是controller节点发送leaderAndISRRequest请求每一个节点,通知该节点上topic-partition对应的详细leader情况以及备份节点情况
   * @param leaderAndISRRequest 接收到的controller发来的请求
   * @param offsetManager
   * @return
   */
  def becomeLeaderOrFollower(leaderAndISRRequest: LeaderAndIsrRequest,
                             offsetManager: OffsetManager): (collection.Map[(String, Int), Short], Short) = {
    leaderAndISRRequest.partitionStateInfos.foreach { case ((topic, partition), stateInfo) =>
      //打印日志,本地节点xxx 收到了partition的leader请求是xxxx,controller节点是xxx,当前controller的选举次数是xxx,正在处理请求是topic:xxx,partition:xxx
      stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, stateInfo, leaderAndISRRequest.correlationId,
                                        leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topic, partition))
    }
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[(String, Int), Short]
      if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {//版本不一致,打印异常信息
        leaderAndISRRequest.partitionStateInfos.foreach { case ((topic, partition), stateInfo) =>
        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
          "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
          leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
        }
        (responseMap, ErrorMapping.StaleControllerEpochCode) //controller的枚举次数不对异常
      } else {
        val controllerId = leaderAndISRRequest.controllerId //请求的controller所在节点
        val correlationId = leaderAndISRRequest.correlationId //请求关联ID
        controllerEpoch = leaderAndISRRequest.controllerEpoch //当前controller枚举次数

        // First check partition's leader epoch 更新每一个partition对应的leader的详细信息
        val partitionState = new HashMap[Partition, PartitionStateInfo]()
        leaderAndISRRequest.partitionStateInfos.foreach{ case ((topic, partitionId), partitionStateInfo) =>
          val partition = getOrCreatePartition(topic, partitionId)
          val partitionLeaderEpoch = partition.getLeaderEpoch() //当前节点记录的leader的枚举次数
          // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
          // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
          if (partitionLeaderEpoch < partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch) {//说明leader节点校验是有效的
            if(partitionStateInfo.allReplicas.contains(config.brokerId))//说明该节点存在该partition的备份集合里面的
              partitionState.put(partition, partitionStateInfo)
            else {//说明该partition备份集合没有该节点,因此打印警告日志即可
              stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topic, partition.partitionId, partitionStateInfo.allReplicas.mkString(",")))
            }
          } else {
            // Otherwise record the error code in response 说明该请求无效,因为本地的leader的枚举次数比请求的大
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] since its associated leader epoch %d is old. Current leader epoch is %d")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
              topic, partition.partitionId, partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch, partitionLeaderEpoch))
            responseMap.put((topic, partitionId), ErrorMapping.StaleLeaderEpochCode) //说明本地的partition的leader枚举次数比请求的还要大,因此说明该topic-partition有问题
          }
        }

        //过滤该节点是partition的leader的集合
        val partitionsTobeLeader = partitionState
          .filter{ case (partition, partitionStateInfo) => partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader == config.brokerId}
        //剩余的该节点是作为partition的follow节点集合
        val partitionsToBeFollower = (partitionState -- partitionsTobeLeader.keys)

        //处理leader集合
        if (!partitionsTobeLeader.isEmpty)
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, leaderAndISRRequest.correlationId, responseMap, offsetManager)
          
          //处理follow集合
        if (!partitionsToBeFollower.isEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, leaderAndISRRequest.leaders, leaderAndISRRequest.correlationId, responseMap, offsetManager)

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        //只有第一次收到leaderAndISRRequest请求后,才会开启线程,防止没有partition的数据,空向线程写入数据
        if (!hwThreadInitialized) {
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }
        replicaFetcherManager.shutdownIdleFetcherThreads()
        (responseMap, ErrorMapping.NoError)
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   * partitionState集合内所有topic-partitio的leader节点都是本节点
   * 1. Stop fetchers for these partitions 停止抓去这些partition信息,因为这些partition已经是leader了,不需要在去抓去了
   * 2. Update the partition metadata in cache 更新partition的元数据信息
   * 3. Add these partitions to the leader partitions set 添加这些partition到leader集合中
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,//controller节点ID,即哪个节点发过来的请求
                          epoch: Int,//controller的枚举次数
                          partitionState: Map[Partition, PartitionStateInfo],//当前要在该节点做leader的partition和leader的详细信息映射
                          correlationId: Int,//客户端发送过来的请求的关联ID
                          responseMap: mutable.Map[(String, Int), Short],//为每一个有异常的topic-partition提供异常状态码
                          offsetManager: OffsetManager) = {
    partitionState.foreach(state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId))))

    for (partition <- partitionState.keys)
      responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError)

    try {
      // First stop fetchers for all the partitions 停止抓取这些partition
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)))
      partitionState.foreach { state =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(state._1.topic, state._1.partitionId)))
      }
      // Update the partition information to be the leader
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        partition.makeLeader(controllerId, partitionStateInfo, correlationId, offsetManager)}

    } catch {
      case e: Throwable =>
        partitionState.foreach { state =>
          val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch,
                                                TopicAndPartition(state._1.topic, state._1.partitionId))
          stateChangeLogger.error(errorMsg, e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   * 当前节点变成partitionState集合中所有topic-partition的follower节点
   * 1. Remove these partitions from the leader partitions set.从leader集合中移除这些partition,因为这些partition已经是follower了
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it
   * 该partition在该节点是follow节点,说明在该节点上一定要有日志存储信息
   */
  private def makeFollowers(controllerId: Int,//controller的节点ID
                            epoch: Int, //controller的枚举次数
                            partitionState: Map[Partition, PartitionStateInfo],//follow节点的集合
                            leaders: Set[Broker], //leader节点集合
                            correlationId: Int, //request请求的关联ID
                            responseMap: mutable.Map[(String, Int), Short],//返回给controller的每一个topic-partition的状态码
                            offsetManager: OffsetManager) {
    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    for (partition <- partitionState.keys)
      responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError)

    try {

      var partitionsToMakeFollower: Set[Partition] = Set()

      // TODO: Delete leaders from LeaderAndIsrRequest in 0.8.1
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
        val newLeaderBrokerId = leaderIsrAndControllerEpoch.leaderAndIsr.leader //新的leaer节点
        leaders.find(_.id == newLeaderBrokerId) match {
          // Only change partition state when the leader is available 当leader是可用的,才会更改partition的状态
          case Some(leaderBroker) =>
            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId, offsetManager))
              partitionsToMakeFollower += partition
            else
            //因为新的leader和老的leader是一样的,因此返回false
              stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                "controller %d epoch %d for partition [%s,%d] since the new leader %d is the same as the old leader")
                .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
                partition.topic, partition.partitionId, newLeaderBrokerId))
          case None =>
            // The leader broker should always be present in the leaderAndIsrRequest.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
              " %d epoch %d for partition [%s,%d] but cannot become follower since the new leader %d is unavailable.")
              .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
              partition.topic, partition.partitionId, newLeaderBrokerId))
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.getOrCreateReplica()
        }
      }

      //因为leader已经更换了,因此以前抓去的地方已经错误了,因此要先停止抓去
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(new TopicAndPartition(_)))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }

      //截短,将大于参数序号的message都删除掉,获取该topic-partition在本地的备份对象,并且知道该备份对象已经同步到哪里了
      logManager.truncateTo(partitionsToMakeFollower.map(partition => (new TopicAndPartition(partition), partition.getOrCreateReplica().highWatermark.messageOffset)).toMap)

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition [%s,%d] as part of " +
          "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
          partition.topic, partition.partitionId, correlationId, controllerId, epoch))
      }

      if (isShuttingDown.get()) { //如果该节点正在关闭中,则不用在去追加这些leader数据了
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition [%s,%d] since it is shutting down").format(localBrokerId, correlationId,
            controllerId, epoch, partition.topic, partition.partitionId))
        }
      }
      else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          new TopicAndPartition(partition) -> BrokerAndInitialOffset(
            leaders.find(_.id == partition.leaderReplicaIdOpt.get).get,
            partition.getReplica().get.logEndOffset.messageOffset)).toMap//找到每一个partition在本地的日志,去进行抓去

        //添加抓去这些partition,去leader节点抓去数据
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset) //参数Map[TopicAndPartition, BrokerAndInitialOffset]

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
            "%d epoch %d with correlation id %d for partition [%s,%d]")
            .format(localBrokerId, controllerId, epoch, correlationId, partition.topic, partition.partitionId))
        }
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }
  }

   /**
   * 评估卡住的同步对象集合
   * 所谓卡住的原因是:1.长时间没有从leader收到同步信息 2.收到的leader的同步信息数据较少
   * @config.replicaLagTimeMaxMs 表示最长时间不能从leader接收信息阀值
   * @config.replicaLagMaxMessages 表示从leader节点同步数据的最大字节长度阀值
   * 
   * 该方法表示收缩同步集合,因为有一些同步节点有问题,导致不再向该集合发送同步数据
   */
  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs, config.replicaLagMaxMessages))
  }

  //follow节点抓取partition的Replica数据后.更新文件偏移量信息LogOffsetMetadata
  //抓取的是topic-parition在replicaId节点上更新的数据量
  //记录该follower节点replicaId,已经同步给他了每一个topic-partition到哪个offset了
  /**
   * follow节点replicaId 已经抓去topic-partition到什么位置了LogOffsetMetadata
   * @param topic
   * @param partitionId
   * @param replicaId
   * @param offset
   */
  def updateReplicaLEOAndPartitionHW(topic: String, partitionId: Int, replicaId: Int, offset: LogOffsetMetadata) = {
    getPartition(topic, partitionId) match {
      case Some(partition) =>
        partition.getReplica(replicaId) match {
          case Some(replica) =>
            replica.logEndOffset = offset
            // check if we need to update HW and expand Isr 更新leader的信息以及可能扩展该备份节点到同步节点集合中
            partition.updateLeaderHWAndMaybeExpandIsr(replicaId)
            debug("Recorded follower %d position %d for partition [%s,%d].".format(replicaId, offset.messageOffset, topic, partitionId))
          case None =>
            throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
              " is not recognized to be one of the assigned replicas %s for partition [%s,%d]").format(localBrokerId, replicaId,
              offset.messageOffset, partition.assignedReplicas().map(_.brokerId).mkString(","), topic, partitionId))

        }
      case None =>
        warn("While recording the follower position, the partition [%s,%d] hasn't been created, skip updating leader HW".format(topic, partitionId))
    }
  }

  //在本节点的所有topic-partition中获取所有是leader的partition集合
  private def getLeaderPartitions() : List[Partition] = {
    allPartitions.values.filter(_.leaderReplicaIfLocal().isDefined).toList//过滤leader节点必须是本地的集合
  }
  /**
   * Flushes the highwatermark value for all partitions to the highwatermark file
   * 向replication-offset-checkpoint文件写入每一个topic-paritition对应的处理过的文件偏移量
   * 该文件存储的值表示该partition所有的同步节点集合至少也同步到什么位置了
   */
  def checkpointHighWatermarks() {
    //返回本地节点的所有replica备份对象
    val replicas = allPartitions.values.map(_.getReplica(config.brokerId)).collect{case Some(replica) => replica}
    // Map[String, Iterable[Replica]] 按照log的根目录分组,key是log根目录,value是该目录下的所有Replica对象
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath)
    for((dir, reps) <- replicasByDir) {
      //Map[TopicAndPartition, Long] key是TopicAndPartition对象,value是该partiton的messageOffset
      val hwms = reps.map(r => (new TopicAndPartition(r) -> r.highWatermark.messageOffset)).toMap
      try {
        //向replication-offset-checkpoint文件写入key是TopicAndPartition对象,value是该partiton的messageOffset信息
        highWatermarkCheckpoints(dir).write(hwms)
      } catch {
        case e: IOException =>
          fatal("Error writing to highwatermark file: ", e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def shutdown() {
    info("Shut down")
    replicaFetcherManager.shutdown()
    checkpointHighWatermarks()
    info("Shut down completely")
  }
}
