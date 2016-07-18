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

package kafka.consumer

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.client.ClientUtils
import kafka.cluster._
import kafka.common._
import kafka.metrics._
import kafka.network.BlockingChannel
import kafka.serializer._
import kafka.utils.Utils.inLock
import kafka.utils.ZkUtils._
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState

import scala.collection._


/**
 * This class handles the consumers interaction with zookeeper
 * 这个类出来消费者与zookeeper之间的交互
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 *  这个类是客户端调用生成的类,作用是通过配置文件ConsumerConfig与服务端交互
 */
private[kafka] object ZookeeperConsumerConnector {
  val shutdownCommand: FetchedDataChunk = new FetchedDataChunk(null, null, -1L)
}

//消费者,基于zookeeper的消费者
private[kafka] class ZookeeperConsumerConnector(val config: ConsumerConfig,
                                                val enableFetcher: Boolean) // for testing only
        extends ConsumerConnector with Logging with KafkaMetricsGroup {

  private val isShuttingDown = new AtomicBoolean(false)
  private val rebalanceLock = new Object
  private var fetcher: Option[ConsumerFetcherManager] = None//消费者抓去数据的类
  private var zkClient: ZkClient = null
  private var topicRegistry = new Pool[String, Pool[Int, PartitionTopicInfo]]
  private val checkpointedZkOffsets = new Pool[TopicAndPartition, Long]//表示某个topic-partition提交到zookeeper到哪个序号了
  private val topicThreadIdAndQueues = new Pool[(String, ConsumerThreadId), BlockingQueue[FetchedDataChunk]]
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "kafka-consumer-scheduler-")
  private val messageStreamCreated = new AtomicBoolean(false)

  private var sessionExpirationListener: ZKSessionExpireListener = null
  private var topicPartitionChangeListener: ZKTopicPartitionChangeListener = null
  private var loadBalancerListener: ZKRebalancerListener = null

  private var offsetsChannel: BlockingChannel = null//获取连接该group对应的服务器上已经提交到哪个message的主broker服务器的BlockingChannel对象
  private val offsetsChannelLock = new Object

  private var wildcardTopicWatcher: ZookeeperTopicEventWatcher = null

  // useful for tracking migration of consumers to store offsets in kafka
  private val kafkaCommitMeter = newMeter("KafkaCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId))
  private val zkCommitMeter = newMeter("ZooKeeperCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId))
  private val rebalanceTimer = new KafkaTimer(newTimer("RebalanceRateAndTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("clientId" -> config.clientId)))

  //设置消费者ID,格式是config.groupId_config.consumerId或者消费者本地ip-时间戳-uuid_config.consumerId
  val consumerIdString = {
    var consumerUuid : String = null
    config.consumerId match {
      case Some(consumerId) // for testing only
      => consumerUuid = consumerId
      case None // generate unique consumerId automatically
      => val uuid = UUID.randomUUID()
      consumerUuid = "%s-%d-%s".format(
        InetAddress.getLocalHost.getHostName, System.currentTimeMillis,
        uuid.getMostSignificantBits().toHexString.substring(0,8))
    }
    config.groupId + "_" + consumerUuid
  }
  this.logIdent = "[" + consumerIdString + "], "

  connectZk()
  //为该消费者创建数据抓取管理对象ConsumerFetcherManager
  createFetcher()
  ensureOffsetManagerConnected()

  if (config.autoCommitEnable) {//表示客户端会自动提交message序号,表示客户端已经抓去完了
    scheduler.startup
    info("starting auto committer every " + config.autoCommitIntervalMs + " ms")
    scheduler.schedule("kafka-consumer-autocommit",
                       autoCommit,
                       delay = config.autoCommitIntervalMs,
                       period = config.autoCommitIntervalMs,//客户端多久去自动提交一下message的序号
                       unit = TimeUnit.MILLISECONDS)
  }

  KafkaMetricsReporter.startReporters(config.props)
  AppInfo.registerInfo()

  def this(config: ConsumerConfig) = this(config, true)

  def createMessageStreams(topicCountMap: Map[String,Int]): Map[String, List[KafkaStream[Array[Byte],Array[Byte]]]] =
    createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder())

  def createMessageStreams[K,V](topicCountMap: Map[String,Int], keyDecoder: Decoder[K], valueDecoder: Decoder[V])
      : Map[String, List[KafkaStream[K,V]]] = {
    if (messageStreamCreated.getAndSet(true))
      throw new MessageStreamsExistException(this.getClass.getSimpleName +
                                   " can create message streams at most once",null)
    consume(topicCountMap, keyDecoder, valueDecoder)
  }

  def createMessageStreamsByFilter[K,V](topicFilter: TopicFilter,
                                        numStreams: Int,
                                        keyDecoder: Decoder[K] = new DefaultDecoder(),
                                        valueDecoder: Decoder[V] = new DefaultDecoder()) = {
    val wildcardStreamsHandler = new WildcardStreamsHandler[K,V](topicFilter, numStreams, keyDecoder, valueDecoder)
    wildcardStreamsHandler.streams
  }

  //为消费者创建一个抓取管理对象
  private def createFetcher() {
    if (enableFetcher)
      fetcher = Some(new ConsumerFetcherManager(consumerIdString, config, zkClient))
  }

  //连接zookeeper
  private def connectZk() {
    info("Connecting to zookeeper instance at " + config.zkConnect)
    zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
  }

  // Blocks until the offset manager is located and a channel is established to it.
  //阻塞,直到连接上服务器
  /**
a.首先连接zookeeper任意一台机器
b.向链接的任意一台服务器发送申请,申请属于该group的元数据去哪台broker获取
c.服务端返回该broker信息
d.客户端再次向新的broker发送请求,返回BlockingChannel对象
   */
  private def ensureOffsetManagerConnected() {
    if (config.offsetsStorage == "kafka") {
      if (offsetsChannel == null || !offsetsChannel.isConnected)
        offsetsChannel = ClientUtils.channelToOffsetManager(config.groupId, zkClient, config.offsetsChannelSocketTimeoutMs, config.offsetsChannelBackoffMs)

      debug("Connected to offset manager %s:%d.".format(offsetsChannel.host, offsetsChannel.port))
    }
  }

  def shutdown() {
    val canShutdown = isShuttingDown.compareAndSet(false, true)
    if (canShutdown) {
      info("ZKConsumerConnector shutting down")
      val startTime = System.nanoTime()
      KafkaMetricsGroup.removeAllConsumerMetrics(config.clientId)
      rebalanceLock synchronized {
        if (wildcardTopicWatcher != null)
          wildcardTopicWatcher.shutdown()
        try {
          if (config.autoCommitEnable)
	        scheduler.shutdown()
          fetcher match {
            case Some(f) => f.stopConnections
            case None =>
          }
          sendShutdownToAllQueues()
          if (config.autoCommitEnable)
            commitOffsets(true)
          if (zkClient != null) {
            zkClient.close()
            zkClient = null
          }

          if (offsetsChannel != null) offsetsChannel.disconnect()
        } catch {
          case e: Throwable =>
            fatal("error during consumer connector shutdown", e)
        }
        info("ZKConsumerConnector shutdown completed in " + (System.nanoTime() - startTime) / 1000000 + " ms")
      }
    }
  }

  /**
   * topicCountMap:key是要抓取的topic,value是需要几个消费者抓取该topic,即因为是一个Map结构,也就是说消费者可以一次抓取多个topic信息
   */
  def consume[K, V](topicCountMap: scala.collection.Map[String,Int], keyDecoder: Decoder[K], valueDecoder: Decoder[V])
      : Map[String,List[KafkaStream[K,V]]] = {
    debug("entering consume ")
    if (topicCountMap == null)
      throw new RuntimeException("topicCountMap is null")

    val topicCount = TopicCount.constructTopicCount(consumerIdString, topicCountMap) //StaticTopicCount

    //返回HashMap[String, Set[ConsumerThreadId]] key是topic,value是该topic上多个线程ID可以读取topic信息
    val topicThreadIds = topicCount.getConsumerThreadIdsPerTopic

    // make a list of (queue,stream) pairs, one pair for each threadId
    /**
     * 返回值List[(LinkedBlockingQueue[FetchedDataChunk], KafkaStream[K, V])],一个元组集合,每一个元组由(LinkedBlockingQueue[FetchedDataChunk], KafkaStream[K, V])组成,每一个元组表示一个线程,按照顺序排列
     * LinkedBlockingQueue[FetchedDataChunk] 表示存储该topic的每一个线程对应的队列
     * KafkaStream[K, V] 表示存储该topic的每一个线程对应的流
     * 
     * eg:读取topic:test1,2个线程,读取topic:test2,3个线程
     * 最终返回List包含5个元组
     */
    val queuesAndStreams = topicThreadIds.values.map(threadIdSet =>
      threadIdSet.map(_ => {
        val queue =  new LinkedBlockingQueue[FetchedDataChunk](config.queuedMaxMessages)
        val stream = new KafkaStream[K,V](
          queue, config.consumerTimeoutMs, keyDecoder, valueDecoder, config.clientId)
        (queue, stream)
      })
    ).flatten.toList

    val dirs = new ZKGroupDirs(config.groupId)
    registerConsumerInZK(dirs, consumerIdString, topicCount)
    reinitializeConsumer(topicCount, queuesAndStreams)

    loadBalancerListener.kafkaMessageAndMetadataStreams.asInstanceOf[Map[String, List[KafkaStream[K,V]]]]
  }

  // this API is used by unit tests only
  def getTopicRegistry: Pool[String, Pool[Int, PartitionTopicInfo]] = topicRegistry

  /**
   * 在/consumers/${group}/ids/consumerIdString节点上存储以下内容
   * 存储内容:
   *        {
      “version”:1,
      “subscription”:{“test_kafka”:3},//订阅topic列表
      “topic名称”: consumer中topic消费者线程数[与队列的分区数量有关]
      “pattern”:”static”,
      “timestamp”:”1416810012297″
      } 
   */
  private def registerConsumerInZK(dirs: ZKGroupDirs, consumerIdString: String, topicCount: TopicCount) {
    info("begin registering consumer " + consumerIdString + " in ZK")
    val timestamp = SystemTime.milliseconds.toString
    val consumerRegistrationInfo = Json.encode(Map("version" -> 1, "subscription" -> topicCount.getTopicCountMap, "pattern" -> topicCount.pattern,
                                                  "timestamp" -> timestamp))

    createEphemeralPathExpectConflictHandleZKBug(zkClient, dirs.consumerRegistryDir + "/" + consumerIdString, consumerRegistrationInfo, null,
                                                 (consumerZKString, consumer) => true, config.zkSessionTimeoutMs)
    info("end registering consumer " + consumerIdString + " in ZK")
  }

  private def sendShutdownToAllQueues() = {
    for (queue <- topicThreadIdAndQueues.values.toSet[BlockingQueue[FetchedDataChunk]]) {
      debug("Clearing up queue")
      queue.clear()
      queue.put(ZookeeperConsumerConnector.shutdownCommand)
      debug("Cleared queue and sent shutdown command")
    }
  }

  def autoCommit() {
    trace("auto committing")
    try {
      commitOffsets(isAutoCommit = false)
    }
    catch {
      case t: Throwable =>
      // log it and let it go
        error("exception during autoCommit: ", t)
    }
  }

  //将消费到什么位置了,写入到zookeeper中
  def commitOffsetToZooKeeper(topicPartition: TopicAndPartition, offset: Long) {
    if (checkpointedZkOffsets.get(topicPartition) != offset) {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic)
      updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + topicPartition.partition, offset.toString)//更新zookeeper内容
      checkpointedZkOffsets.put(topicPartition, offset)//记录该topic-partition提交到zookeeper已经到offset序号了
      zkCommitMeter.mark()
    }
  }

  //true表示成功提交了,false表示没提交
  def commitOffsets(isAutoCommit: Boolean) {
    var retriesRemaining = 1 + (if (isAutoCommit) config.offsetsCommitMaxRetries else 0) // no retries for commits from auto-commit
    var done = false

    while (!done) {
      //true表示提交完成,完成不代表成功,有可能失败了,但是不需要重试,也是完成true
      val committed = offsetsChannelLock synchronized { // committed when we receive either no error codes or only MetadataTooLarge errors
        val offsetsToCommit = immutable.Map(topicRegistry.flatMap { case (topic, partitionTopicInfos) =>
          partitionTopicInfos.map { case (partition, info) =>
            TopicAndPartition(info.topic, info.partitionId) -> OffsetAndMetadata(info.getConsumeOffset())
          }
        }.toSeq:_*)

        if (offsetsToCommit.size > 0) {
          if (config.offsetsStorage == "zookeeper") {//存储在zookeeper中
            offsetsToCommit.foreach { case(topicAndPartition, offsetAndMetadata) =>
              commitOffsetToZooKeeper(topicAndPartition, offsetAndMetadata.offset)
            }
            true
          } else {//说明是存储到kafka的某个topic下
            val offsetCommitRequest = OffsetCommitRequest(config.groupId, offsetsToCommit, clientId = config.clientId)
            ensureOffsetManagerConnected()//确保连接了该topic-partition,作为生产者,向kafka写入数据
            try {
              kafkaCommitMeter.mark(offsetsToCommit.size)
              offsetsChannel.send(offsetCommitRequest)//发送请求
              val offsetCommitResponse = OffsetCommitResponse.readFrom(offsetsChannel.receive().buffer)//收到回复
              trace("Offset commit response: %s.".format(offsetCommitResponse))

              //定义四个变量
              val (commitFailed, retryableIfFailed, shouldRefreshCoordinator, errorCount) = {
                offsetCommitResponse.commitStatus.foldLeft(false, false, false, 0) { case(folded, (topicPartition, errorCode)) =>

                  if (errorCode == ErrorMapping.NoError && config.dualCommitEnabled) {//同时也向zookeeper写入
                      val offset = offsetsToCommit(topicPartition).offset
                      commitOffsetToZooKeeper(topicPartition, offset)
                  }

                  (folded._1 || // update commitFailed
                     errorCode != ErrorMapping.NoError,

                  folded._2 || // update retryableIfFailed - (only metadata too large is not retryable) 仅仅当元数据太长的时候是不允许重试的,其他都失败的时候更新,让其重试
                    (errorCode != ErrorMapping.NoError && errorCode != ErrorMapping.OffsetMetadataTooLargeCode),

                  folded._3 || // update shouldRefreshCoordinator 应该重新连接topic
                    errorCode == ErrorMapping.NotCoordinatorForConsumerCode ||
                    errorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode,

                  // update error count 更新错误数量
                  folded._4 + (if (errorCode != ErrorMapping.NoError) 1 else 0))
                }
              }
              debug(errorCount + " errors in offset commit response.")


              if (shouldRefreshCoordinator) {//应该重新连接
                debug("Could not commit offsets (because offset coordinator has moved or is unavailable).")
                offsetsChannel.disconnect()
              }

              if (commitFailed && retryableIfFailed)//需要重试,虽然失败了,因此返回false,说明本次没成功
                false
              else
                true
            }
            catch {
              case t: Throwable =>
                error("Error while committing offsets.", t)
                offsetsChannel.disconnect()
                false
            }
          }
        } else {
          debug("No updates to offsets since last commit.")
          true
        }
      }

      done = if (isShuttingDown.get() && isAutoCommit) { // should not retry indefinitely if shutting down
        retriesRemaining -= 1
        retriesRemaining == 0 || committed
      } else
        true

      if (!done) {
        debug("Retrying offset commit in %d ms".format(config.offsetsChannelBackoffMs))
        Thread.sleep(config.offsetsChannelBackoffMs)
      }
    }
  }

  /**
   * KAFKA-1743: This method added for backward compatibility.
   */
  def commitOffsets { commitOffsets(true) }

  private def fetchOffsetFromZooKeeper(topicPartition: TopicAndPartition) = {
    val dirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic)
    val offsetString = readDataMaybeNull(zkClient, dirs.consumerOffsetDir + "/" + topicPartition.partition)._1
    offsetString match {
      case Some(offsetStr) => (topicPartition, OffsetMetadataAndError(offsetStr.toLong, OffsetAndMetadata.NoMetadata, ErrorMapping.NoError))
      case None => (topicPartition, OffsetMetadataAndError.NoOffset)
    }
  }

  private def fetchOffsets(partitions: Seq[TopicAndPartition]) = {
    if (partitions.isEmpty)
      Some(OffsetFetchResponse(Map.empty))
    else if (config.offsetsStorage == "zookeeper") {
      val offsets = partitions.map(fetchOffsetFromZooKeeper)
      Some(OffsetFetchResponse(immutable.Map(offsets:_*)))
    } else {
      val offsetFetchRequest = OffsetFetchRequest(groupId = config.groupId, requestInfo = partitions, clientId = config.clientId)

      var offsetFetchResponseOpt: Option[OffsetFetchResponse] = None
      while (!isShuttingDown.get && !offsetFetchResponseOpt.isDefined) {
        offsetFetchResponseOpt = offsetsChannelLock synchronized {
          ensureOffsetManagerConnected()
          try {
            offsetsChannel.send(offsetFetchRequest)
            val offsetFetchResponse = OffsetFetchResponse.readFrom(offsetsChannel.receive().buffer)
            trace("Offset fetch response: %s.".format(offsetFetchResponse))

            val (leaderChanged, loadInProgress) =
              offsetFetchResponse.requestInfo.foldLeft(false, false) { case(folded, (topicPartition, offsetMetadataAndError)) =>
                (folded._1 || (offsetMetadataAndError.error == ErrorMapping.NotCoordinatorForConsumerCode),
                 folded._2 || (offsetMetadataAndError.error == ErrorMapping.OffsetsLoadInProgressCode))
              }

            if (leaderChanged) {
              offsetsChannel.disconnect()
              debug("Could not fetch offsets (because offset manager has moved).")
              None // retry
            }
            else if (loadInProgress) {
              debug("Could not fetch offsets (because offset cache is being loaded).")
              None // retry
            }
            else {
              if (config.dualCommitEnabled) {
                // if dual-commit is enabled (i.e., if a consumer group is migrating offsets to kafka), then pick the
                // maximum between offsets in zookeeper and kafka.
                val kafkaOffsets = offsetFetchResponse.requestInfo
                val mostRecentOffsets = kafkaOffsets.map { case (topicPartition, kafkaOffset) =>
                  val zkOffset = fetchOffsetFromZooKeeper(topicPartition)._2.offset
                  val mostRecentOffset = zkOffset.max(kafkaOffset.offset)
                  (topicPartition, OffsetMetadataAndError(mostRecentOffset, kafkaOffset.metadata, ErrorMapping.NoError))
                }
                Some(OffsetFetchResponse(mostRecentOffsets))
              }
              else
                Some(offsetFetchResponse)
            }
          }
          catch {
            case e: Exception =>
              warn("Error while fetching offsets from %s:%d. Possible cause: %s".format(offsetsChannel.host, offsetsChannel.port, e.getMessage))
              offsetsChannel.disconnect()
              None // retry
          }
        }

        if (offsetFetchResponseOpt.isEmpty) {
          debug("Retrying offset fetch in %d ms".format(config.offsetsChannelBackoffMs))
          Thread.sleep(config.offsetsChannelBackoffMs)
        }
      }

      offsetFetchResponseOpt
    }
  }


  class ZKSessionExpireListener(val dirs: ZKGroupDirs,
                                 val consumerIdString: String,
                                 val topicCount: TopicCount,
                                 val loadBalancerListener: ZKRebalancerListener)
    extends IZkStateListener {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      /**
       *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
       *  connection for us. We need to release the ownership of the current consumer and re-register this
       *  consumer in the consumer registry and trigger a rebalance.
       */
      info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString)
      loadBalancerListener.resetState()
      registerConsumerInZK(dirs, consumerIdString, topicCount)
      // explicitly trigger load balancing for this consumer
      loadBalancerListener.syncedRebalance()
      // There is no need to resubscribe to child and state changes.
      // The child change watchers will be set inside rebalance when we read the children list.
    }

  }

  class ZKTopicPartitionChangeListener(val loadBalancerListener: ZKRebalancerListener)
    extends IZkDataListener {

    def handleDataChange(dataPath : String, data: Object) {
      try {
        info("Topic info for path " + dataPath + " changed to " + data.toString + ", triggering rebalance")
        // queue up the rebalance event
        loadBalancerListener.rebalanceEventTriggered()
        // There is no need to re-subscribe the watcher since it will be automatically
        // re-registered upon firing of this event by zkClient
      } catch {
        case e: Throwable => error("Error while handling topic partition change for data path " + dataPath, e )
      }
    }

    @throws(classOf[Exception])
    def handleDataDeleted(dataPath : String) {
      // TODO: This need to be implemented when we support delete topic
      warn("Topic for path " + dataPath + " gets deleted, which should not happen at this time")
    }
  }

  /**
   * 消费者group要监控zookeeper,保证在有变化的时候,可以及时更改
   * @group 消费者组名称
   * @consumerIdString 消费者名称
   */
  class ZKRebalancerListener(val group: String, val consumerIdString: String,
                             val kafkaMessageAndMetadataStreams: mutable.Map[String,List[KafkaStream[_,_]]])
    extends IZkChildListener {

    //创建消费者组向多个线程去分配partition的策略
    private val partitionAssignor = PartitionAssignor.createInstance(config.partitionAssignmentStrategy)

    private var isWatcherTriggered = false
    private val lock = new ReentrantLock
    private val cond = lock.newCondition()

    @volatile private var allTopicsOwnedPartitionsCount = 0
    newGauge("OwnedPartitionsCount",
      new Gauge[Int] {
        def value() = allTopicsOwnedPartitionsCount
      },
      Map("clientId" -> config.clientId, "groupId" -> config.groupId))

    private def ownedPartitionsCountMetricTags(topic: String) = Map("clientId" -> config.clientId, "groupId" -> config.groupId, "topic" -> topic)

    private val watcherExecutorThread = new Thread(consumerIdString + "_watcher_executor") {
      override def run() {
        info("starting watcher executor thread for consumer " + consumerIdString)
        var doRebalance = false
        while (!isShuttingDown.get) {
          try {
            lock.lock()
            try {
              if (!isWatcherTriggered)
                cond.await(1000, TimeUnit.MILLISECONDS) // wake up periodically so that it can check the shutdown flag
            } finally {
              doRebalance = isWatcherTriggered
              isWatcherTriggered = false
              lock.unlock()
            }
            if (doRebalance)
              syncedRebalance
          } catch {
            case t: Throwable => error("error during syncedRebalance", t)
          }
        }
        info("stopping watcher executor thread for consumer " + consumerIdString)
      }
    }
    watcherExecutorThread.start()

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      rebalanceEventTriggered()
    }

    def rebalanceEventTriggered() {
      inLock(lock) {
        isWatcherTriggered = true
        cond.signalAll()
      }
    }

    private def deletePartitionOwnershipFromZK(topic: String, partition: Int) {
      val topicDirs = new ZKGroupTopicDirs(group, topic)
      val znode = topicDirs.consumerOwnerDir + "/" + partition
      deletePath(zkClient, znode)
      debug("Consumer " + consumerIdString + " releasing " + znode)
    }

    private def releasePartitionOwnership(localTopicRegistry: Pool[String, Pool[Int, PartitionTopicInfo]])= {
      info("Releasing partition ownership")
      for ((topic, infos) <- localTopicRegistry) {
        for(partition <- infos.keys) {
          deletePartitionOwnershipFromZK(topic, partition)
        }
        removeMetric("OwnedPartitionsCount", ownedPartitionsCountMetricTags(topic))
        localTopicRegistry.remove(topic)
      }
      allTopicsOwnedPartitionsCount = 0
    }

    def resetState() {
      topicRegistry.clear
    }

    def syncedRebalance() {
      rebalanceLock synchronized {
        rebalanceTimer.time {
          if(isShuttingDown.get())  {
            return
          } else {
            for (i <- 0 until config.rebalanceMaxRetries) {
              info("begin rebalancing consumer " + consumerIdString + " try #" + i)
              var done = false
              var cluster: Cluster = null
              try {
                cluster = getCluster(zkClient)//读取所有brokers节点,并且创建Broker节点对象
                done = rebalance(cluster)
              } catch {
                case e: Throwable =>
                  /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
                    * For example, a ZK node can disappear between the time we get all children and the time we try to get
                    * the value of a child. Just let this go since another rebalance will be triggered.
                    **/
                  info("exception during rebalance ", e)
              }
              info("end rebalancing consumer " + consumerIdString + " try #" + i)
              if (done) {
                return
              } else {
                /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
                 * clear the cache 重新平衡失败,在下一次重新平衡操作前要清空缓存*/
                info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered")
              }
              // stop all fetchers and clear all the queues to avoid data duplication
              closeFetchersForQueues(cluster, kafkaMessageAndMetadataStreams, topicThreadIdAndQueues.map(q => q._2))
              Thread.sleep(config.rebalanceBackoffMs)
            }
          }
        }
      }

      throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.rebalanceMaxRetries +" retries")
    }

    private def rebalance(cluster: Cluster): Boolean = {
      val myTopicThreadIdsMap = TopicCount.constructTopicCount(
        group, consumerIdString, zkClient, config.excludeInternalTopics).getConsumerThreadIdsPerTopic
        //获取/brokers/ids所有节点,并且过滤非有效的broker对象,获取当前集群中合法的broker的对象集合.并且已经排序后返回
      val brokers = getAllBrokersInCluster(zkClient)
      if (brokers.size == 0) {
        // This can happen in a rare case when there are no brokers available in the cluster when the consumer is started.
        // We log an warning and register for child changes on brokers/id so that rebalance can be triggered when the brokers
        // are up.
        warn("no brokers found when trying to rebalance.")
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, loadBalancerListener)
        true
      }
      else {
        /**
         * fetchers must be stopped to avoid data duplication, since if the current
         * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
         * But if we don't stop the fetchers first, this consumer would continue returning data for released
         * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
         */
        closeFetchers(cluster, kafkaMessageAndMetadataStreams, myTopicThreadIdsMap)

        releasePartitionOwnership(topicRegistry)

        val assignmentContext = new AssignmentContext(group, consumerIdString, config.excludeInternalTopics, zkClient)
        val partitionOwnershipDecision = partitionAssignor.assign(assignmentContext)
        val currentTopicRegistry = new Pool[String, Pool[Int, PartitionTopicInfo]](
          valueFactory = Some((topic: String) => new Pool[Int, PartitionTopicInfo]))

        // fetch current offsets for all topic-partitions
        val topicPartitions = partitionOwnershipDecision.keySet.toSeq

        val offsetFetchResponseOpt = fetchOffsets(topicPartitions)

        if (isShuttingDown.get || !offsetFetchResponseOpt.isDefined)
          false
        else {
          val offsetFetchResponse = offsetFetchResponseOpt.get
          topicPartitions.foreach(topicAndPartition => {
            val (topic, partition) = topicAndPartition.asTuple
            val offset = offsetFetchResponse.requestInfo(topicAndPartition).offset
            val threadId = partitionOwnershipDecision(topicAndPartition)
            addPartitionTopicInfo(currentTopicRegistry, partition, topic, offset, threadId)
          })

          /**
           * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
           * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
           */
          if(reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
            allTopicsOwnedPartitionsCount = partitionOwnershipDecision.size

            partitionOwnershipDecision.view.groupBy { case(topicPartition, consumerThreadId) => topicPartition.topic }
                                      .foreach { case (topic, partitionThreadPairs) =>
              newGauge("OwnedPartitionsCount",
                new Gauge[Int] {
                  def value() = partitionThreadPairs.size
                },
                ownedPartitionsCountMetricTags(topic))
            }

            topicRegistry = currentTopicRegistry
            updateFetcher(cluster)
            true
          } else {
            false
          }
        }
      }
    }

    private def closeFetchersForQueues(cluster: Cluster,
                                       messageStreams: Map[String,List[KafkaStream[_,_]]],
                                       queuesToBeCleared: Iterable[BlockingQueue[FetchedDataChunk]]) {
      val allPartitionInfos = topicRegistry.values.map(p => p.values).flatten
      fetcher match {
        case Some(f) =>
          f.stopConnections
          clearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, messageStreams)
          info("Committing all offsets after clearing the fetcher queues")
          /**
          * here, we need to commit offsets before stopping the consumer from returning any more messages
          * from the current data chunk. Since partition ownership is not yet released, this commit offsets
          * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
          * for the current data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
          * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
          * successfully and the fetchers restart to fetch more data chunks
          **/
        if (config.autoCommitEnable)
          commitOffsets(true)
        case None =>
      }
    }

    private def clearFetcherQueues(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster,
                                   queuesTobeCleared: Iterable[BlockingQueue[FetchedDataChunk]],
                                   messageStreams: Map[String,List[KafkaStream[_,_]]]) {

      // Clear all but the currently iterated upon chunk in the consumer thread's queue
      queuesTobeCleared.foreach(_.clear)
      info("Cleared all relevant queues for this fetcher")

      // Also clear the currently iterated upon chunk in the consumer threads
      if(messageStreams != null)
         messageStreams.foreach(_._2.foreach(s => s.clear()))

      info("Cleared the data chunks in all the consumer message iterators")

    }

    private def closeFetchers(cluster: Cluster, messageStreams: Map[String,List[KafkaStream[_,_]]],
                              relevantTopicThreadIdsMap: Map[String, Set[ConsumerThreadId]]) {
      // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer
      // after this rebalancing attempt
      val queuesTobeCleared = topicThreadIdAndQueues.filter(q => relevantTopicThreadIdsMap.contains(q._1._1)).map(q => q._2)
      closeFetchersForQueues(cluster, messageStreams, queuesTobeCleared)
    }

    private def updateFetcher(cluster: Cluster) {
      // update partitions for fetcher
      var allPartitionInfos : List[PartitionTopicInfo] = Nil
      for (partitionInfos <- topicRegistry.values)
        for (partition <- partitionInfos.values)
          allPartitionInfos ::= partition
      info("Consumer " + consumerIdString + " selected partitions : " +
        allPartitionInfos.sortWith((s,t) => s.partitionId < t.partitionId).map(_.toString).mkString(","))

      fetcher match {
        case Some(f) =>
          f.startConnections(allPartitionInfos, cluster)
        case None =>
      }
    }

    private def reflectPartitionOwnershipDecision(partitionOwnershipDecision: Map[TopicAndPartition, ConsumerThreadId]): Boolean = {
      var successfullyOwnedPartitions : List[(String, Int)] = Nil
      val partitionOwnershipSuccessful = partitionOwnershipDecision.map { partitionOwner =>
        val topic = partitionOwner._1.topic
        val partition = partitionOwner._1.partition
        val consumerThreadId = partitionOwner._2
        val partitionOwnerPath = getConsumerPartitionOwnerPath(group, topic, partition)
        try {
          createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId.toString)
          info(consumerThreadId + " successfully owned partition " + partition + " for topic " + topic)
          successfullyOwnedPartitions ::= (topic, partition)
          true
        } catch {
          case e: ZkNodeExistsException =>
            // The node hasn't been deleted by the original owner. So wait a bit and retry.
            info("waiting for the partition ownership to be deleted: " + partition)
            false
          case e2: Throwable => throw e2
        }
      }
      val hasPartitionOwnershipFailed = partitionOwnershipSuccessful.foldLeft(0)((sum, decision) => sum + (if(decision) 0 else 1))
      /* even if one of the partition ownership attempt has failed, return false */
      if(hasPartitionOwnershipFailed > 0) {
        // remove all paths that we have owned in ZK
        successfullyOwnedPartitions.foreach(topicAndPartition => deletePartitionOwnershipFromZK(topicAndPartition._1, topicAndPartition._2))
        false
      }
      else true
    }

    private def addPartitionTopicInfo(currentTopicRegistry: Pool[String, Pool[Int, PartitionTopicInfo]],
                                      partition: Int, topic: String,
                                      offset: Long, consumerThreadId: ConsumerThreadId) {
      val partTopicInfoMap = currentTopicRegistry.getAndMaybePut(topic)

      val queue = topicThreadIdAndQueues.get((topic, consumerThreadId))
      val consumedOffset = new AtomicLong(offset)
      val fetchedOffset = new AtomicLong(offset)
      val partTopicInfo = new PartitionTopicInfo(topic,
                                                 partition,
                                                 queue,
                                                 consumedOffset,
                                                 fetchedOffset,
                                                 new AtomicInteger(config.fetchMessageMaxBytes),
                                                 config.clientId)
      partTopicInfoMap.put(partition, partTopicInfo)
      debug(partTopicInfo + " selected new offset " + offset)
      checkpointedZkOffsets.put(TopicAndPartition(topic, partition), offset)
    }
  }

  /**
   * @queuesAndStreams 表示一个元组队列,没一个元组分别对应一个topic的一个线程
   */
  private def reinitializeConsumer[K,V](
      topicCount: TopicCount,
      queuesAndStreams: List[(LinkedBlockingQueue[FetchedDataChunk],KafkaStream[K,V])]) {

    val dirs = new ZKGroupDirs(config.groupId)

    // listener to consumer and partition changes
    if (loadBalancerListener == null) {
      val topicStreamsMap = new mutable.HashMap[String,List[KafkaStream[K,V]]]
      loadBalancerListener = new ZKRebalancerListener(
        config.groupId, consumerIdString, topicStreamsMap.asInstanceOf[scala.collection.mutable.Map[String, List[KafkaStream[_,_]]]])
    }

    // create listener for session expired event if not exist yet
    if (sessionExpirationListener == null)
      sessionExpirationListener = new ZKSessionExpireListener(
        dirs, consumerIdString, topicCount, loadBalancerListener)

    // create listener for topic partition change event if not exist yet
    if (topicPartitionChangeListener == null)
      topicPartitionChangeListener = new ZKTopicPartitionChangeListener(loadBalancerListener)

    val topicStreamsMap = loadBalancerListener.kafkaMessageAndMetadataStreams

    // map of {topic -> Set(thread-1, thread-2, ...)}
    val consumerThreadIdsPerTopic: Map[String, Set[ConsumerThreadId]] =
      topicCount.getConsumerThreadIdsPerTopic

    val allQueuesAndStreams = topicCount match {
      case wildTopicCount: WildcardTopicCount =>
        /*
         * Wild-card consumption streams share the same queues, so we need to
         * duplicate the list for the subsequent zip operation.
         */
        (1 to consumerThreadIdsPerTopic.keySet.size).flatMap(_ => queuesAndStreams).toList
      case statTopicCount: StaticTopicCount =>
        queuesAndStreams
    }

    val topicThreadIds = consumerThreadIdsPerTopic.map {
      case(topic, threadIds) =>
        threadIds.map((topic, _))
    }.flatten

    require(topicThreadIds.size == allQueuesAndStreams.size,
      "Mismatch between thread ID count (%d) and queue count (%d)"
      .format(topicThreadIds.size, allQueuesAndStreams.size))
    val threadQueueStreamPairs = topicThreadIds.zip(allQueuesAndStreams)

    threadQueueStreamPairs.foreach(e => {
      val topicThreadId = e._1
      val q = e._2._1
      topicThreadIdAndQueues.put(topicThreadId, q)
      debug("Adding topicThreadId %s and queue %s to topicThreadIdAndQueues data structure".format(topicThreadId, q.toString))
      newGauge(
        "FetchQueueSize",
        new Gauge[Int] {
          def value = q.size
        },
        Map("clientId" -> config.clientId,
          "topic" -> topicThreadId._1,
          "threadId" -> topicThreadId._2.threadId.toString)
      )
    })

    val groupedByTopic = threadQueueStreamPairs.groupBy(_._1._1)
    groupedByTopic.foreach(e => {
      val topic = e._1
      val streams = e._2.map(_._2._2).toList
      topicStreamsMap += (topic -> streams)
      debug("adding topic %s and %d streams to map.".format(topic, streams.size))
    })

    // listener to consumer and partition changes
    zkClient.subscribeStateChanges(sessionExpirationListener)

    zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener)

    topicStreamsMap.foreach { topicAndStreams =>
      // register on broker partition path changes
      val topicPath = BrokerTopicsPath + "/" + topicAndStreams._1
      zkClient.subscribeDataChanges(topicPath, topicPartitionChangeListener)
    }

    // explicitly trigger load balancing for this consumer
    loadBalancerListener.syncedRebalance()
  }

  class WildcardStreamsHandler[K,V](topicFilter: TopicFilter,
                                  numStreams: Int,
                                  keyDecoder: Decoder[K],
                                  valueDecoder: Decoder[V])
                                extends TopicEventHandler[String] {

    //将false变成true
    if (messageStreamCreated.getAndSet(true))
      throw new RuntimeException("Each consumer connector can create " +
        "message streams by filter at most once.")

    //通配符队列和流
    /**
     * 注意val表示不变的变量,初始化后就不会再变化了,再次调用该属性只是会得到一个具体的值而已，不会再进行for循环了
     * 1.循环numStreams次
     * 2.每次e表示第几次,即1 2 3 4 等等
     * 3.每次都产生一个队列LinkedBlockingQueue和一个KafkaStream对象组成的元组(LinkedBlockingQueue,KafkaStream)
     * 4.最终返回一个List
     * List((LinkedBlockingQueue,KafkaStream),(LinkedBlockingQueue,KafkaStream), (LinkedBlockingQueue,KafkaStream))
     * List的size是numStreams
     */
    private val wildcardQueuesAndStreams = (1 to numStreams)
      .map(e => {
        val queue = new LinkedBlockingQueue[FetchedDataChunk](config.queuedMaxMessages)
        val stream = new KafkaStream[K,V](queue,
                                          config.consumerTimeoutMs,
                                          keyDecoder,
                                          valueDecoder,
                                          config.clientId)
        (queue, stream)
    }).toList

     // bootstrap with existing topics 获取所有的topic集合,并且进行topicFilter过滤后的集合
    private var wildcardTopics =
      getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
        .filter(topic => topicFilter.isTopicAllowed(topic, config.excludeInternalTopics))

    //获取该消费者每一个topic要多少个线程去执行
    private val wildcardTopicCount = TopicCount.constructTopicCount(
      consumerIdString, topicFilter, numStreams, zkClient, config.excludeInternalTopics)

    val dirs = new ZKGroupDirs(config.groupId)
    registerConsumerInZK(dirs, consumerIdString, wildcardTopicCount)
    reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams)

    /*
     * Topic events will trigger subsequent synced rebalances.
     * 监听/brokers/topics节点,一旦有topic变更则触发TopicEventHandler事件
     */
    info("Creating topic event watcher for topics " + topicFilter)
    wildcardTopicWatcher = new ZookeeperTopicEventWatcher(zkClient, this)

    //当前所有的topic内容,当初始化和topic有变化的时候被通知
    def handleTopicEvent(allTopics: Seq[String]) {
      debug("Handling topic event")

      //过滤合法的topic集合
      val updatedTopics = allTopics.filter(topic => topicFilter.isTopicAllowed(topic, config.excludeInternalTopics))

      //获取到不在wildcardTopics集合里面的topic,即刚刚新增的topic
      val addedTopics = updatedTopics filterNot (wildcardTopics contains)
      if (addedTopics.nonEmpty)
        info("Topic event: added topics = %s"
                             .format(addedTopics))//说明一定有刚刚新增的topic

      /*
       * TODO: Deleted topics are interesting (and will not be a concern until
       * 0.8 release). We may need to remove these topics from the rebalance
       * listener's map in reinitializeConsumer.
       * 有一些topic已经不存在了,所以要找到wildcardTopics中不包含的topic
       */
      val deletedTopics = wildcardTopics filterNot (updatedTopics contains)
      if (deletedTopics.nonEmpty)
        info("Topic event: deleted topics = %s"
                             .format(deletedTopics))//说明有删除的topic

      wildcardTopics = updatedTopics//更新为最新的topic集合
      info("Topics to consume = %s".format(wildcardTopics))

      if (addedTopics.nonEmpty || deletedTopics.nonEmpty)//重新平衡topic消费
        reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams)
    }

    //返回每一个KafkaStream流的集合
    def streams: Seq[KafkaStream[K,V]] =
      wildcardQueuesAndStreams.map(_._2)//循环wildcardQueuesAndStreams,每个元素进入map方法,最终返回的还是一个集合,只是集合的内容变成_2了
  }
}
