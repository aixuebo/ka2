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

import kafka.admin._
import kafka.log.LogConfig
import kafka.log.CleanerConfig
import kafka.log.LogManager
import kafka.utils._
import java.util.concurrent._
import atomic.{AtomicInteger, AtomicBoolean}
import java.io.File
import org.I0Itec.zkclient.ZkClient
import kafka.controller.{ControllerStats, KafkaController}
import kafka.cluster.Broker
import kafka.api.{ControlledShutdownResponse, ControlledShutdownRequest}
import kafka.common.ErrorMapping
import kafka.network.{Receive, BlockingChannel, SocketServer}
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 * 代表一个kafka的单节点,该类控制该节点的生命周期
 * 处理该节点的启动和关闭服务功能
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Kafka Server " + config.brokerId + "], "
  private var isShuttingDown = new AtomicBoolean(false)
  private var shutdownLatch = new CountDownLatch(1)
  private var startupComplete = new AtomicBoolean(false)//true表示完成了开启状态
  val brokerState: BrokerState = new BrokerState //broker节点状态
  val correlationId: AtomicInteger = new AtomicInteger(0)
  var socketServer: SocketServer = null//服务端
  var requestHandlerPool: KafkaRequestHandlerPool = null
  var logManager: LogManager = null
  var offsetManager: OffsetManager = null //用于在本地服务器记录topic-partition的偏移量信息
  var kafkaHealthcheck: KafkaHealthcheck = null//将该brokerId注册到zookeeper中,健康该broker节点是否健康
  var topicConfigManager: TopicConfigManager = null //为该broker节点更新topic的配置信息
  var replicaManager: ReplicaManager = null
  var apis: KafkaApis = null
  var kafkaController: KafkaController = null
  
  //线程池调度器
  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
  var zkClient: ZkClient = null

  newGauge(
    "BrokerState",
    new Gauge[Int] {
      def value = brokerState.currentState
    }
  )

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    try {
      info("starting")
      brokerState.newState(Starting) //服务器正在开启中
      isShuttingDown = new AtomicBoolean(false)
      shutdownLatch = new CountDownLatch(1)

      /* start scheduler 创建线程池*/
      kafkaScheduler.startup()
    
      /* setup zookeeper 连接zookeeper*/
      zkClient = initZk()

      /* start log manager */
      logManager = createLogManager(zkClient, brokerState)
      logManager.startup()

      socketServer = new SocketServer(config.brokerId,
                                      config.hostName,
                                      config.port,
                                      config.numNetworkThreads,
                                      config.queuedMaxRequests,
                                      config.socketSendBufferBytes,
                                      config.socketReceiveBufferBytes,
                                      config.socketRequestMaxBytes,
                                      config.maxConnectionsPerIp,
                                      config.connectionsMaxIdleMs,
                                      config.maxConnectionsPerIpOverrides)
      socketServer.startup()

      replicaManager = new ReplicaManager(config, time, zkClient, kafkaScheduler, logManager, isShuttingDown)

      /* start offset manager */
      offsetManager = createOffsetManager()

      kafkaController = new KafkaController(config, zkClient, brokerState)
    
      /* start processing requests */
      apis = new KafkaApis(socketServer.requestChannel, replicaManager, offsetManager, zkClient, config.brokerId, config, kafkaController)
      requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
      brokerState.newState(RunningAsBroker)
   
      Mx4jLoader.maybeLoad()

      replicaManager.startup()

      kafkaController.startup()
    
      topicConfigManager = new TopicConfigManager(zkClient, logManager)
      topicConfigManager.startup()
    
      /* tell everyone we are alive */
      kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, config.advertisedHostName, config.advertisedPort, config.zkSessionTimeoutMs, zkClient)
      kafkaHealthcheck.startup()

    
      registerStats()//注册统计相关指标
      startupComplete.set(true)
      info("started")
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  //初始化zookeeper
  private def initZk(): ZkClient = {
    info("Connecting to zookeeper on " + config.zkConnect)

    //连接zookeeper后,创建一个node节点chroot,如果存在chroot,则该参数的目标是确保chroot的path是存在的
    val chroot = {
      if (config.zkConnect.indexOf("/") > 0)
        config.zkConnect.substring(config.zkConnect.indexOf("/"))
      else
        ""
    }

    if (chroot.length > 1) {//说明url中带有chroot
      val zkConnForChrootCreation = config.zkConnect.substring(0, config.zkConnect.indexOf("/"))
      val zkClientForChrootCreation = new ZkClient(zkConnForChrootCreation, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
      ZkUtils.makeSurePersistentPathExists(zkClientForChrootCreation, chroot)
      info("Created zookeeper path " + chroot)
      zkClientForChrootCreation.close()
    }

    val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
    // 初始化,建立持久化的path
    ZkUtils.setupCommonPaths(zkClient)
    zkClient
  }


  /**
   *  Forces some dynamic jmx beans to be registered on server startup.
   *  注册统计相关指标
   */
  private def registerStats() {
    BrokerTopicStats.getBrokerAllTopicsStats()
    ControllerStats.uncleanLeaderElectionRate
    ControllerStats.leaderElectionTimer
  }

  /**
   *  Performs controlled shutdown
   *  向controller发送shutdown命令
   */
  private def controlledShutdown() {
    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      var remainingRetries = config.controlledShutdownMaxRetries //尝试发送次数
      info("Starting controlled shutdown")
      var channel : BlockingChannel = null //与controller节点建立连接
      var prevController : Broker = null //上一次controller是哪个节点
      var shutdownSuceeded : Boolean = false //说明shutdown是否成功了
      try {
        brokerState.newState(PendingControlledShutdown) //该服务器已经准备shutdown ,已经向controller提交了,等待controller回复的过程
        while (!shutdownSuceeded && remainingRetries > 0) {//只要没成功,就不断循环,直到次数到为止
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request
          val controllerId = ZkUtils.getController(zkClient) //读取/controller节点的内容
          //创建与controller的连接
          ZkUtils.getBrokerInfo(zkClient, controllerId) match {
            case Some(broker) =>
              if (channel == null || prevController == null || !prevController.equals(broker)) { // !prevController.equals(broker) 说明controller已经变更了,要重新连接一下,channel == null || prevController == null说明还没有与controller连接,说明是第一次连接
                // if this is the first attempt or if the controller has changed, create a channel to the most recent
                // controller
                if (channel != null) {
                  channel.disconnect()
                }
                channel = new BlockingChannel(broker.host, broker.port,
                  BlockingChannel.UseDefaultBufferSize,
                  BlockingChannel.UseDefaultBufferSize,
                  config.controllerSocketTimeoutMs)//与controller节点建立连接
                channel.connect()
                prevController = broker
              }
            case None=>
              //ignore and try again
          }

          // 2. issue a controlled shutdown to the controller 发送一个shutdown命令给controller节点
          if (channel != null) {
            var response: Receive = null
            try {
              // send the controlled shutdown request
              val request = new ControlledShutdownRequest(correlationId.getAndIncrement, config.brokerId)
              channel.send(request)

              response = channel.receive() //接收controller节点的回复信息
              val shutdownResponse = ControlledShutdownResponse.readFrom(response.buffer)
              if (shutdownResponse.errorCode == ErrorMapping.NoError && shutdownResponse.partitionsRemaining != null &&
                  shutdownResponse.partitionsRemaining.size == 0) {//说明该节点上没有leader的partition了,就说明可以进行shutdown了
                shutdownSuceeded = true //说明shutdown成功了
                info ("Controlled shutdown succeeded")
              }else {
                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.mkString(",")))//打印还是leader的partition集合
                info("Error code from controller: %d".format(shutdownResponse.errorCode))//打印返回状态码
              }
            }
            catch {
              case ioe: java.io.IOException =>
                channel.disconnect()
                channel = null
                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                // ignore and try again
            }
          }
          if (!shutdownSuceeded) {//说明没提交成功,因此休息一会,再发送
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally {
        if (channel != null) {
          channel.disconnect()
          channel = null
        }
      }
      if (!shutdownSuceeded) {//说明最后也没有提交成功,则打印日志
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
      }
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    try {
      info("shutting down")
      val canShutdown = isShuttingDown.compareAndSet(false, true)
      if (canShutdown) {
        Utils.swallow(controlledShutdown())
        brokerState.newState(BrokerShuttingDown)
        if(socketServer != null)
          Utils.swallow(socketServer.shutdown())
        if(requestHandlerPool != null)
          Utils.swallow(requestHandlerPool.shutdown())
        if(offsetManager != null)
          offsetManager.shutdown()
        Utils.swallow(kafkaScheduler.shutdown())
        if(apis != null)
          Utils.swallow(apis.close())
        if(replicaManager != null)
          Utils.swallow(replicaManager.shutdown())
        if(logManager != null)
          Utils.swallow(logManager.shutdown())
        if(kafkaController != null)
          Utils.swallow(kafkaController.shutdown())
        if(zkClient != null)
          Utils.swallow(zkClient.close())

        brokerState.newState(NotRunning)
        shutdownLatch.countDown()
        startupComplete.set(false)
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager
  
  private def createLogManager(zkClient: ZkClient, brokerState: BrokerState): LogManager = {
    val defaultLogConfig = LogConfig(segmentSize = config.logSegmentBytes,
                                     segmentMs = config.logRollTimeMillis,
                                     segmentJitterMs = config.logRollTimeJitterMillis,
                                     flushInterval = config.logFlushIntervalMessages,
                                     flushMs = config.logFlushIntervalMs.toLong,
                                     retentionSize = config.logRetentionBytes,
                                     retentionMs = config.logRetentionTimeMillis,
                                     maxMessageSize = config.messageMaxBytes,
                                     maxIndexSize = config.logIndexSizeMaxBytes,
                                     indexInterval = config.logIndexIntervalBytes,
                                     deleteRetentionMs = config.logCleanerDeleteRetentionMs,
                                     fileDeleteDelayMs = config.logDeleteDelayMs,
                                     minCleanableRatio = config.logCleanerMinCleanRatio,
                                     compact = config.logCleanupPolicy.trim.toLowerCase == "compact")
    val defaultProps = defaultLogConfig.toProps
    
    //针对每一个topic有单独的配置信息,与默认配置信息合并覆盖操作
    //configs是一个Map<String, LogConfig>,可以是topic,value是该topic的LogConfig对象
    val configs = AdminUtils.fetchAllTopicConfigs(zkClient).mapValues(LogConfig.fromProps(defaultProps, _)) ////每一个topic,对应一个独立的配置
    // read the log configurations from zookeeper
    val cleanerConfig = CleanerConfig(numThreads = config.logCleanerThreads,
                                      dedupeBufferSize = config.logCleanerDedupeBufferSize,
                                      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
                                      ioBufferSize = config.logCleanerIoBufferSize,
                                      maxMessageSize = config.messageMaxBytes,
                                      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
                                      backOffMs = config.logCleanerBackoffMs,
                                      enableCleaner = config.logCleanerEnable)
    new LogManager(logDirs = config.logDirs.map(new File(_)).toArray,
                   topicConfigs = configs,
                   defaultConfig = defaultLogConfig,
                   cleanerConfig = cleanerConfig,
                   ioThreads = config.numRecoveryThreadsPerDataDir,
                   flushCheckMs = config.logFlushSchedulerIntervalMs,
                   flushCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
                   retentionCheckMs = config.logCleanupIntervalMs,
                   scheduler = kafkaScheduler,
                   brokerState = brokerState,
                   time = time)
  }

  private def createOffsetManager(): OffsetManager = {
    val offsetManagerConfig = OffsetManagerConfig(
      maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    new OffsetManager(offsetManagerConfig, replicaManager, zkClient, kafkaScheduler)
  }
}