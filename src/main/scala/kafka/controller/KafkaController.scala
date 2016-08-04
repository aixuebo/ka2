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
package kafka.controller

import collection._
import collection.Set
import com.yammer.metrics.core.Gauge
import java.lang.{IllegalStateException, Object}
import java.util.concurrent.TimeUnit
import kafka.admin.AdminUtils
import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.api._
import kafka.cluster.Broker
import kafka.common._
import kafka.log.LogConfig
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import kafka.utils.ZkUtils._
import kafka.utils._
import kafka.utils.Utils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkDataListener, IZkStateListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.None
import kafka.server._
import scala.Some
import kafka.common.TopicAndPartition

/**
 * Controller的上下文对象
 */
class ControllerContext(val zkClient: ZkClient,
                        val zkSessionTimeout: Int) {
  
  var controllerChannelManager: ControllerChannelManager = null
  val controllerLock: ReentrantLock = new ReentrantLock()
  
  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty//已经进行shuttingDown的Broker节点ID集合
  
  val brokerShutdownLock: Object = new Object
  
  //controller_epoch节点的值是一个数字,kafka集群中第一个broker第一次启动时为1，以后只要集群中center controller中央控制器所在broker变更或挂掉，就会重新选举新的center controller，
  // 每次center controller变更controller_epoch值就会 + 1;
  var epoch: Int = KafkaController.InitialControllerEpoch - 1 //真正是从zookeeper中读取的该值
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1 //真正是从zookeeper中读取的该值
  
  val correlationId: AtomicInteger = new AtomicInteger(0)
  
  //存储管理目前有哪些topic集合
  var allTopics: Set[String] = Set.empty
  
  //key是topic-partition对象,value是该partition的备份的ID集合
  var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty
  
  /**
   * 1.循环所有的topic-partition
   * 2.获取每一个/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
   * key是topic-partition value是该partition对应的leader节点等信息对象
   */
  var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty

  /**
   * 管理员设置的每一个topic-partition要分配的备份节点集合
   */
  var partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
  
  //判断是否topic-partition的第一个备份节点是leader节点,如果是则不处理什么,如果不是,则添加到partitionsUndergoingPreferredReplicaElection中
  var partitionsUndergoingPreferredReplicaElection: mutable.Set[TopicAndPartition] = new mutable.HashSet

  private var liveBrokersUnderlying: Set[Broker] = Set.empty//目前存活的Broker节点集合
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty//目前存活的Broker节点ID集合

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter从活着的节点集合中,过滤掉shuttingDownBrokerIds节点
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))//过滤掉shuttingDownBrokerIds节点后,获取当前依然还存活的Broker节点集合
  def liveBrokerIds = liveBrokerIdsUnderlying.filter(brokerId => !shuttingDownBrokerIds.contains(brokerId))//过滤掉shuttingDownBrokerIds节点后,获取当前依然还存活的Broker节点id集合

  //获取当前存活的节点以及正在shuttingDown的节点集合
  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  
  /**
   * 1.循环参数所有的节点集合
   * 2.筛选节点集合,如果节点存在备份数据,则保留
   * 3.返回TopicAndPartition集合,即  该对象记录一个topic-partition集合
   * 
   * 即返回给定节点集合中存在的TopicAndPartition集合
   */
  def partitionsOnBroker(brokerId: Int): Set[TopicAndPartition] = {
    partitionReplicaAssignment
      .filter { case(topicAndPartition, replicas) => replicas.contains(brokerId) }
      .map { case(topicAndPartition, replicas) => topicAndPartition }
      .toSet
  }

  /**
   * 1.循环参数所有的节点集合
   * 2.筛选节点集合,如果节点存在备份数据,则保留
   * 3.返回PartitionAndReplica集合,即  该对象记录一个topic-partition-在哪些broker上
   */
  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.map { brokerId =>
      partitionReplicaAssignment.filter { case(topicAndPartition, replicas) => replicas.contains(brokerId) }.map { case(topicAndPartition, replicas) =>
                 new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId) }
    }.flatten.toSet
  }

  //通过topic获取备份集合
  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case(topicAndPartition, replicas) => topicAndPartition.topic.equals(topic) }
      .map { case(topicAndPartition, replicas) =>
        replicas.map { r =>
          new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)
        }
    }.flatten.toSet
  }

  //获取指定topic对应的所有TopicAndPartition对象,即该topic有多少个partition
  def partitionsForTopic(topic: String): collection.Set[TopicAndPartition] = {
    partitionReplicaAssignment
      .filter { case(topicAndPartition, replicas) => topicAndPartition.topic.equals(topic) }.keySet
  }

  /**
   * 返回PartitionAndReplica集合,即  该对象记录一个topic-partition-在broker上有备份,即参数replica就是备份的brokerId
   * 即返回活着的节点中,哪些节点存在topic-partition备份信息,则返回该详细信息集合
   */
  def allLiveReplicas(): Set[PartitionAndReplica] = {
    //参数:过滤掉shuttingDownBrokerIds节点后,获取当前依然还存活的Broker节点id集合
    replicasOnBrokers(liveBrokerIds)
  }

  //通过topic-partition获取备份集合
  def replicasForPartition(partitions: collection.Set[TopicAndPartition]): collection.Set[PartitionAndReplica] = {
    partitions.map { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }.flatten
  }

  //删除掉该topic的信息
  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    allTopics -= topic
  }
}

/**
 * 解析kafka的controller节点
 */
object KafkaController extends Logging {
  val stateChangeLogger = new StateChangeLogger("state.change.logger")
  val InitialControllerEpoch = 1 //记录当前第几次选择变更leader节点
  val InitialControllerEpochZkVersion = 1 //记录zookeeper上版本号

  case class StateChangeLogger(override val loggerName: String) extends Logging

  //参数controllerInfoString:是json格式,解析的内容是哪个broker节点是kafka主节点,返回该主节点的broker的id
  /**
   * 新版本是json,格式是brokerid:节点id
   * 老版本不是json,直接存储的是brokerid
   */
  def parseControllerId(controllerInfoString: String): Int = {
    try {
      Json.parseFull(controllerInfoString) match {
        case Some(m) =>
          val controllerInfo = m.asInstanceOf[Map[String, Any]]
          return controllerInfo.get("brokerid").get.asInstanceOf[Int]
        case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
      }
    } catch {
      //json解析失败,可能是使用老版本的kafka,老版本存储的值就是一个int,因此将该值强转成int,如果依然失败,则抛异常说明解析不了
      case t: Throwable =>
        // It may be due to an incompatible controller register version
        warn("Failed to parse the controller info as json. "
          + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
        try {
          return controllerInfoString.toInt
        } catch {
          case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
        }
    }
  }
}

class KafkaController(val config : KafkaConfig, zkClient: ZkClient, val brokerState: BrokerState) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Controller " + config.brokerId + "]: "
  private var isRunning = true
  
  private val stateChangeLogger = KafkaController.stateChangeLogger//log对象
  val controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs)//创建上下文对象
  
  val partitionStateMachine = new PartitionStateMachine(this)//partition的状态机
  val replicaStateMachine = new ReplicaStateMachine(this)//paritition的备份对象的状态机
  
  private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
    onControllerResignation, config.brokerId)
  
  // have a separate scheduler for the controller to be able to start and stop independently of the
  // kafka server单线程任务调度器,用于使用单独的线程执行一个无参数、无返回值的函数
  private val autoRebalanceScheduler = new KafkaScheduler(1)
  
  var deleteTopicManager: TopicDeletionManager = null
  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config)
  private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
  private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
  private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this) //处理 批处理请求 的对象

  //监听zookeeper的/admin/reassign_partitions节点
  private val partitionReassignedListener = new PartitionsReassignedListener(this)
  private val preferredReplicaElectionListener = new PreferredReplicaElectionListener(this)

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value() = if (isActive) 1 else 0
    }
  )

  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value(): Int = {
        inLock(controllerContext.controllerLock) {
          if (!isActive())
            0
          else
            controllerContext.partitionLeadershipInfo.count(p => !controllerContext.liveOrShuttingDownBrokerIds.contains(p._2.leaderAndIsr.leader))
        }
      }
    }
  )

  newGauge(
    "PreferredReplicaImbalanceCount",
    new Gauge[Int] {
      def value(): Int = {
        inLock(controllerContext.controllerLock) {
          if (!isActive())
            0
          else
            controllerContext.partitionReplicaAssignment.count {
              case (topicPartition, replicas) => controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != replicas.head
            }
        }
      }
    }
  )

  def epoch = controllerContext.epoch

  //设置客户端ID
  def clientId = "id_%d-host_%s-port_%d".format(config.brokerId, config.hostName, config.port)

  /**
   * On clean shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   * 在要对节点shutdown的时候,首先找到该节点上是leader的partition,将这些partition的leader迁移到同步节点中的一个里面
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.返回该节点上仍然是leader的partition集合
   */
  def shutdownBroker(id: Int) : Set[TopicAndPartition] = {

    if (!isActive()) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    controllerContext.brokerShutdownLock synchronized {
      info("Shutting down broker " + id)

      inLock(controllerContext.controllerLock) {
        if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id)) //确保此时该节点是存在的
          throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

        controllerContext.shuttingDownBrokerIds.add(id) //该节点添加到关闭节点集合中
        debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
        debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))
      }

      /**
       * controllerContext.partitionsOnBroker(id) 返回给定节点集合中存在的TopicAndPartition集合
       * .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size)) 返回该节点上存在的topic-partition对应的备份因子数量
       */
      val allPartitionsAndReplicationFactorOnBroker: Set[(TopicAndPartition, Int)] =
        inLock(controllerContext.controllerLock) {
          controllerContext.partitionsOnBroker(id)
            .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size))
        }

      allPartitionsAndReplicationFactorOnBroker.foreach {
        case(topicAndPartition, replicationFactor) =>
          // Move leadership serially to relinquish lock.
          inLock(controllerContext.controllerLock) {
            controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
              if (replicationFactor > 1) {//说明备份节点因子>1
                if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {//说明该节点是该partition的leader节点所在节点
                  // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
                  // notifies all affected brokers
                  //重新选择一个新的leader
                  partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                    controlledShutdownPartitionLeaderSelector)
                } else {//说明该节点不是partition的leader节点
                  // Stop the replica first. The state change below initiates ZK changes which should take some time
                  // before which the stop replica request should be completed (in most cases)
                  brokerRequestBatch.newBatch()
                  //向id节点发送信息,通知他不再对topic-partition进行备份了
                  brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
                    topicAndPartition.partition, deletePartition = false)
                  //触发请求发送
                  brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement)

                  // If the broker is a follower, updates the isr in ZK and notifies the current leader
                  replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                    topicAndPartition.partition, id)), OfflineReplica)
                }
              }
            }
          }
      }
      def replicatedPartitionsBrokerLeads() = inLock(controllerContext.controllerLock) {
        trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(",")) //打印所有的leader
        controllerContext.partitionLeadershipInfo.filter {
          case (topicAndPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
        }.map(_._1)
      }
      replicatedPartitionsBrokerLeads().toSet
    }
  }

  /**
   * 当前broker被选举成新的controller时,调用该回调方法
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 当前broker变成controller状态的时候,会执行以下逻辑
   * 1. Register controller epoch changed listener 注册各种监听
   * 2. Increments the controller epoch 增加第几次更换controller次数
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager 开启该程序执行
   * 5. Starts the replica state machine 开启该程序执行
   * 6. Starts the partition state machine 开启该程序执行
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * 如果遭遇到了任何不期望的异常或者错误,则重新分配conraoller
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   * 这个机制确保了其他controller 选举将会被触发,然后总是活跃
   * 
   * 当该机器broker变成leader时候执行该函数
   */
  def onControllerFailover() {
    if(isRunning) {
      //打印日志,说明该broker 已经开始切换成主要的controller了
      info("Broker %d starting become controller state transition".format(config.brokerId))
      //read controller epoch from zk 初始化/controller_epoch节点,获取该节点存储的已经第几次更改controller
      readControllerEpochFromZookeeper()
      // increment the controller epoch 设置/controller_epoch节点,使该节点存储的已经第几次更改controller累加1
      incrementControllerEpoch(zkClient)
      // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
      registerReassignedPartitionsListener()//为/admin/reassign_partitions节点注册事件PartitionsReassignedListener
      registerPreferredReplicaElectionListener()///admin/preferred_replica_election节点添加监听PreferredReplicaElectionListener
      partitionStateMachine.registerListeners()//添加/brokers/topics节点监听
      replicaStateMachine.registerListeners()//注册监听/brokers/ids
      
      //初始化ControllerContext的信息
      initializeControllerContext()
      
      //开启任务
      replicaStateMachine.startup()
      partitionStateMachine.startup()
      // register the partition change listeners for all existing topics on failover
      //为每一个topic添加监听,监听该topic的partition是否有变化,即partition是否增加或者减少了,或者备份节点集合是否变化
      controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
      info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))//打印说明该节点已经是controller了
      brokerState.newState(RunningAsController)//设置该服务器运行中,并且该服务器也已经是controller服务器了
      maybeTriggerPartitionReassignment()
      maybeTriggerPreferredReplicaElection()
      /* send partition leadership info to all live brokers
      * 向活着的节点发送partition的leader信息
      **/
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
      if (config.autoLeaderRebalanceEnable) {
        info("starting the partition rebalance scheduler")
        autoRebalanceScheduler.startup()
        autoRebalanceScheduler.schedule("partition-rebalance-thread", checkAndTriggerPartitionRebalance,
          5, config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS)
      }
      deleteTopicManager.start()
    }
    else
      info("Controller has been shut down, aborting startup/failover")
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   * 当该机器broker重新进行leader分配的时候执行该函数
   */
  def onControllerResignation() {
    // de-register listeners
    deregisterReassignedPartitionsListener()
    deregisterPreferredReplicaElectionListener()

    // shutdown delete topic manager
    if (deleteTopicManager != null)
      deleteTopicManager.shutdown()

    // shutdown leader rebalance scheduler
    if (config.autoLeaderRebalanceEnable)
      autoRebalanceScheduler.shutdown()

    inLock(controllerContext.controllerLock) {
      // de-register partition ISR listener for on-going partition reassignment task
      deregisterReassignedPartitionsIsrChangeListeners()
      // shutdown partition state machine
      partitionStateMachine.shutdown()
      // shutdown replica state machine
      replicaStateMachine.shutdown()
      // shutdown controller channel manager
      if(controllerContext.controllerChannelManager != null) {
        controllerContext.controllerChannelManager.shutdown()
        controllerContext.controllerChannelManager = null
      }
      // reset controller context
      controllerContext.epoch=0
      controllerContext.epochZkVersion=0
      brokerState.newState(RunningAsBroker)
    }
  }

  /**
   * Returns true if this broker is the current controller.
   * true表示该broker是当前的controller节点
   */
  def isActive(): Boolean = {
    inLock(controllerContext.controllerLock) {
      controllerContext.controllerChannelManager != null
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 当新增节点加入的时候,调用该方法,是ReplicaStateMachine类对/brokers/ids节点进行监听,监听是否有新增节点或者删除节点时触发的
   * 1. Triggers the OnlinePartition state change for all new/offline partitions
   * 2. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))
    val newBrokersSet = newBrokers.toSet
    // send update metadata request for all partitions to the newly restarted brokers. In cases of controlled shutdown
    // leaders will not be elected when a new broker comes up. So at least in the common controlled shutdown case, the
    // metadata will reach the new brokers faster
    sendUpdateMetadataRequest(newBrokers)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
      case (topicAndPartition, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
    }
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.size > 0) {
      info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
        "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
        deleteTopicManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
      deleteTopicManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It does the following -
   * 当节点被删除的时候,调用该方法,是ReplicaStateMachine类对/brokers/ids节点进行监听,监听是否有新增节点或者删除节点时触发的
   * 1. Mark partitions with dead leaders as offline
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
   * the partition state machine will refresh our cache for us when performing leader election for all new/offline
   * partitions coming online.
   */
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
    val deadBrokersSet = deadBrokers.toSet
    // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader) &&
        !deleteTopicManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // filter out the replicas that belong to topics that are being deleted
    var allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet)
    val activeReplicasOnDeadBrokers = allReplicasOnDeadBrokers.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    // handle dead replicas
    replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica)
    // check if topic deletion state for the dead replicas needs to be updated
    val replicasForTopicsToBeDeleted = allReplicasOnDeadBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.size > 0) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      deleteTopicManager.failReplicaDeletion(replicasForTopicsToBeDeleted)
    }
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of new topics
   * and partitions as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
   * 
   * 参数topics 是新添加的topic集合
   * 参数newPartitions,因为每一个topic有若干个partition,因此新添加的topic集合,就有N个topic-partition集合
   * 作用:
   * 对新添加的topic进行分配,将topic-partition分配到不同节点中
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic)) //为新增加的topic增加partition内容监听
    onNewPartitionCreation(newPartitions) //添加新的partition
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   * 某个topic增加了一些partition
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition) //先将新增加的partition状态改成new
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector) //将new的状态改成OnlinePartition
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RAR = Reassigned replicas
   * OAR = Original list of replicas for partition
   * AR = current assigned replicas
   *
   * 1. Update AR in ZK with OAR + RAR.
   * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
   *    of the leader epoch in zookeeper.
   * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
   * 4. Wait until all replicas in RAR are in sync with the leader.
   * 5  Move all replicas in RAR to OnlineReplica state.
   * 6. Set AR to RAR in memory.
   * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
   *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
   *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
   *    RAR - OAR back in the isr.
   * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *    isr to remove OAR - RAR in zookeeper and sent a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
   * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = false) to
   *    the replicas in OAR - RAR to physically delete the replicas on disk.
   * 10. Update AR in ZK with RAR.
   * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
   * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
   * may go through the following transition.
   * AR                 leader/isr
   * {1,2,3}            1/{1,2,3}           (initial state)
   * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
   * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
   * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
   * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
   * {4,5,6}            4/{4,5,6}           (step 10)
   *
   * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas) match {
      case false =>
        //false说明新添加的reassignedReplicas集合,已经都在该partition的备份集合内了,不需要再次添加
        info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned not yet caught up with the leader")
        val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet //新集合中不在内存映射的节点集合
        val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet//新老集合之和
        //1. Update AR in ZK with OAR + RAR.
        //为/brokers/topics/${topic}节点写入最新内容{partitions:{"1":[11,12,14],"2":[11,16,19]} }
        updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)
        //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
        updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
          newAndOldReplicas.toSeq)
        //3. replicas in RAR - OAR -> NewReplica
        startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
        info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned to catch up with the leader")
      case true =>
        //4. Wait until all replicas in RAR are in sync with the leader.
        val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
        //5. replicas in RAR -> OnlineReplica
        reassignedReplicas.foreach { replica =>
          replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
            replica)), OnlineReplica)
        }
        //6. Set AR to RAR in memory.
        //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
        //   a new AR (using RAR) and same isr to every broker in RAR
        moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
        //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
        //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
        stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)
        //10. Update AR in ZK with RAR.
        updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)
        //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
        removePartitionFromReassignedPartitions(topicAndPartition)
        info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
        //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
        sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
        // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
        deleteTopicManager.resumeDeletionForTopics(Set(topicAndPartition.topic))
    }
  }

  private def watchIsrChangesForReassignedPartition(topic: String,
                                                    partition: Int,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas //新的备份节点集合
    val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
      reassignedReplicas.toSet)
    reassignedPartitionContext.isrChangeListener = isrChangeListener //设置监听器
    // register listener on the leader and isr path to wait until they catch up with the current leader
    //zookeeper在/brokers/topics/${topic}/partitions/${partitionId}/state注册监听器
    zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
  }

  /**
   * 为topic-partition添加一组新的备份节点,属于管理员管理设置范畴
   */
  def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                        reassignedPartitionContext: ReassignedPartitionsContext) {
    val newReplicas = reassignedPartitionContext.newReplicas //新的备份节点集合
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    val aliveNewReplicas = newReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))//保留活着的节点集合
    try {
      val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition) //获取该topic-partition已经分配的节点集合
      assignedReplicasOpt match {
        case Some(assignedReplicas) =>
          if(assignedReplicas == newReplicas) {//说明相同,不需要重新分配
            throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
              " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
          } else {//此时说明不相同,说明要去替换备份节点集合了
            if(aliveNewReplicas == newReplicas) {//说明相同,表示新的备份节点集合都是活着的节点
              info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
              // first register ISR change listener
              watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
              controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)//添加管理中
              // mark topic ineligible for deletion for the partitions being reassigned
              deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
              onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
            } else {//说明管理员配置的新的备份节点集合有死的节点,因此抛异常
              // some replica in RAR is not alive. Fail partition reassignment
              throw new KafkaException("Only %s replicas out of the new set of replicas".format(aliveNewReplicas.mkString(",")) +
                " %s for partition %s to be reassigned are alive. ".format(newReplicas.mkString(","), topicAndPartition) +
                "Failing partition reassignment")
            }
          }
        case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
          .format(topicAndPartition))
      }
    } catch {
      case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
      // remove the partition from the admin path to unblock the admin client
      removePartitionFromReassignedPartitions(topicAndPartition)
    }
  }

  //添加一组管理员设置的leader级别的topic-partition集合
  def onPreferredReplicaElection(partitions: Set[TopicAndPartition], isTriggeredByAutoRebalance: Boolean = false) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    try {
      controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions//全局中添加映射
      deleteTopicManager.markTopicIneligibleForDeletion(partitions.map(_.topic))
      partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
    } catch {
      case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
    } finally {
      removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
      deleteTopicManager.resumeDeletionForTopics(partitions.map(_.topic))
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    inLock(controllerContext.controllerLock) {
      info("Controller starting up");
      registerSessionExpirationListener()
      isRunning = true
      controllerElector.startup
      info("Controller startup complete")
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    inLock(controllerContext.controllerLock) {
      isRunning = false
    }
    onControllerResignation()
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  //设置/controller_epoch节点,使该节点存储的已经第几次更改controller累加1
  def incrementControllerEpoch(zkClient: ZkClient) = {
    try {
      var newControllerEpoch = controllerContext.epoch + 1
            //更新/controller_epoch节点的值,是原来值+1
      val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPathIfExists(zkClient,
        ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
      if(!updateSucceeded)
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure") //更新失败抛异常
      else {
        //更新之后重新设置新值和当前zookeeper的version信息
        controllerContext.epochZkVersion = newVersion
        controllerContext.epoch = newControllerEpoch
      }
    } catch {
      //如果/controller_epoch节点不存在异常,因此说明是第一次初始化,因此要将其值设置为1即可
      case nne: ZkNoNodeException =>
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        try {
          zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
          controllerContext.epoch = KafkaController.InitialControllerEpoch
          controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
        } catch {//说明在设置的时候,有其他节点注册了,因此抛异常即可
          case e: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
            "Aborting controller startup procedure")
          case oe: Throwable => error("Error while incrementing controller epoch", oe)
        }
      case oe: Throwable => error("Error while incrementing controller epoch", oe)

    }
    info("Controller %d incremented epoch to %d".format(config.brokerId, controllerContext.epoch))
  }

  private def registerSessionExpirationListener() = {
    zkClient.subscribeStateChanges(new SessionExpirationListener())
  }

  /**
   * 1.获取/brokers/ids所有节点,并且过滤非有效的broker对象,获取当前集群中合法的broker的对象集合.并且已经排序后返回
   * 2.获取所有的topic集合,/brokers/topics
   * 3./brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
   *   返回值 Map[TopicAndPartition, Seq[Int]] key是topic-partition,value是该partition都在哪些节点有备份
   * 4.为每一个topic-partition,映射LeaderIsrAndControllerEpoch对象,该对象可以获取该partition的leader等信息
   */
  private def initializeControllerContext() {
    // update controller cache with delete topic information
    controllerContext.liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet //获取/brokers/ids所有节点,并且过滤非有效的broker对象,获取当前集群中合法的broker的对象集合.并且已经排序后返回
    controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).toSet//获取所有的topic集合,/brokers/topics
    /**
   * 1.读取/brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
   * 2.解析内容,返回映射关系
   * 返回值 Map[TopicAndPartition, Seq[Int]] key是topic-partition,value是该partition都在哪些节点有备份
     */
    controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, controllerContext.allTopics.toSeq)
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    /**
     * 1.循环所有的topic-partition
     * 2.获取每一个/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象,该对象可以获取该partition的leader等信息
     */
    updateLeaderAndIsrCache()
    // start the channel manager 
    startChannelManager() //创建ControllerChannelManager对象,并且执行startUp方法
    initializePreferredReplicaElection()
    initializePartitionReassignment()
    initializeTopicDeletion()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  //读取/admin/preferred_replica_election节点信息存储的topic-partition集合
  //判断是否topic-partition的第一个备份节点是leader节点,如果是则不处理什么,如果不是,则添加到partitionsUndergoingPreferredReplicaElection中
  private def initializePreferredReplicaElection() {
    // initialize preferred replica election state 读取/admin/preferred_replica_election节点信息存储的topic-partition集合,即Set[TopicAndPartition],返回管理员设置的leader级别的partition
    val partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient)
    // check if they are already completed or topic was deleted
    //检查每一个设置的topic-partition是否已经完成了第一个备份就是leader,或者该topic已经被删除了,没有partition了
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition)//获取该partition对应的备份节点集合
      val topicDeleted = replicasOpt.isEmpty
      //如果该partition的备份数量>0,则successful判断第一个备份节点是否是该partition的leader
      val successful =
        if(!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false
      successful || topicDeleted
    }

    //先将全部管理员设置的leader级别的topic-partition添加到映射中
    controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection
    //删除掉已经第一个备份节点就是管理员要求的leader节点的映射
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
  }

  //重新分配每一个topic-partition对应的备份节点
  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    /**
     * 1.获取/admin/reassign_partitions的内容,即管理员设置的每一个topic-partition对应哪些备份节点
     * 2.重新分配topic-partition,参数newReplicas是重新分配的brokerId集合,
     *  返回Map[TopicAndPartition, ReassignedPartitionsContext]
     */
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
    // check if they are already completed or topic was deleted
    //循环每一个topic-partition,获取目前该topic-parttion对应已经分配的备份节点集合
    val reassignedPartitions = partitionsBeingReassigned.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition._1)//获取目前该topic-parttion对应已经分配的备份节点集合
      val topicDeleted = replicasOpt.isEmpty
      //true表示当前存在的备份集合与设置新的备份集合相同
      val successful = if(!topicDeleted) replicasOpt.get == partition._2.newReplicas else false
      topicDeleted || successful
    }.map(_._1)//获取当前现状与管理员期望的相同的
    reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
    var partitionsToReassign: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
    partitionsToReassign ++= partitionsBeingReassigned //先添加全部
    partitionsToReassign --= reassignedPartitions//再减去目前与管理员期望相同的,剩余就是还尚未完成的
    controllerContext.partitionsBeingReassigned ++= partitionsToReassign
    info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
    info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
    info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()))
  }

  private def initializeTopicDeletion() {
    val topicsQueuedForDeletion = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.DeleteTopicsPath).toSet//已经删除的topic集合
    /**
     * 找到broker节点已经死了,但是在该节点上还有备份的topic集合
     */
    val topicsWithReplicasOnDeadBrokers = controllerContext.partitionReplicaAssignment.filter { case(partition, replicas) =>
      replicas.exists(r => !controllerContext.liveBrokerIds.contains(r)) }.keySet.map(_.topic)
    //说明该topic管理员还为其分配了partition的leader节点集合
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic)
    //说明管理员还为该topic分配partition集合
    val topicsForWhichPreferredReplicaElectionIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)

    val topicsIneligibleForDeletion = topicsWithReplicasOnDeadBrokers | topicsForWhichPartitionReassignmentIsInProgress |
                                  topicsForWhichPreferredReplicaElectionIsInProgress
    info("List of topics to be deleted: %s".format(topicsQueuedForDeletion.mkString(",")))
    info("List of topics ineligible for deletion: %s".format(topicsIneligibleForDeletion.mkString(",")))
    // initialize the topic deletion manager
    deleteTopicManager = new TopicDeletionManager(this, topicsQueuedForDeletion, topicsIneligibleForDeletion)
  }

  private def maybeTriggerPartitionReassignment() {
    controllerContext.partitionsBeingReassigned.foreach { topicPartitionToReassign =>
      initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
    }
  }

  private def maybeTriggerPreferredReplicaElection() {
    onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet)
  }

  //创建ControllerChannelManager对象,并且执行startUp方法
  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config)
    controllerContext.controllerChannelManager.startup()
  }

  /**
   * 1.循环所有的topic-partition
   * 2.获取每一个/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
   */
  private def updateLeaderAndIsrCache() {
    val leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet)
    for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
  }

  /**
   * 1.查找topic-partition目前已经有的备份节点集合
   * 2.过滤已经存在的集合中,参数2replicas不在已经存在的集合内的集合
   * 3.true表示存在等待新添加的集合
   */
  private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
    //获取/brokers/topics/ ${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
    getLeaderAndIsrForPartition(zkClient, topic, partition) match {
      case Some(leaderAndIsr) =>
        val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r))
        replicasNotInIsr.isEmpty
      case None => false
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
    } else {
      // check if the leader is alive or not
      controllerContext.liveBrokerIds.contains(currentLeader) match {
        case true =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
          // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
          updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas)
        case false =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
          partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = oldReplicas.map(r => PartitionAndReplica(topic, partition, r))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  //为/brokers/topics/${topic}节点写入最新内容{partitions:{"1":[11,12,14],"2":[11,16,19]} }
  private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                 replicas: Seq[Int]) {
    //过滤查找所有属于该topic的所有partition
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
    //重新更新该partition对应的备份节点集合
    partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas)
    //为/brokers/topics/${topic}节点写入最新内容{partitions:{"1":[11,12,14],"2":[11,16,19]} }
    updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
    info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, replicas.mkString(",")))
    // update the assigned replica list after a successful zookeeper write 更新内存映射
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas)
  }

  private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicAndPartition: TopicAndPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    brokerRequestBatch.newBatch()
    updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
          topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas)
        brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch, controllerContext.correlationId.getAndIncrement)
        stateChangeLogger.trace(("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
          "to leader %d for partition being reassigned %s").format(config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
          newAssignedReplicas.mkString(","), updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition))
      case None => // fail the reassignment
        stateChangeLogger.error(("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
          "to leader for partition being reassigned %s").format(config.brokerId, controllerContext.epoch,
          newAssignedReplicas.mkString(","), topicAndPartition))
    }
  }

  //为/admin/reassign_partitions节点注册事件PartitionsReassignedListener
  private def registerReassignedPartitionsListener() = {
    zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
  }

  //解除/admin/reassign_partitions节点监听
  private def deregisterReassignedPartitionsListener() = {
    zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
  }

  ///admin/preferred_replica_election节点添加监听PreferredReplicaElectionListener
  private def registerPreferredReplicaElectionListener() {
    zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  //解除/admin/preferred_replica_election节点监听
  private def deregisterPreferredReplicaElectionListener() {
    zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterReassignedPartitionsIsrChangeListeners() {
    controllerContext.partitionsBeingReassigned.foreach {
      case (topicAndPartition, reassignedPartitionsContext) =>
        val zkPartitionPath = ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition)
        zkClient.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener)
    }
  }

  //初始化/controller_epoch节点,获取该节点存储的已经第几次更改controller
  private def readControllerEpochFromZookeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    if(ZkUtils.pathExists(controllerContext.zkClient, ZkUtils.ControllerEpochPath)) {
      val epochData = ZkUtils.readData(controllerContext.zkClient, ZkUtils.ControllerEpochPath)
      controllerContext.epoch = epochData._1.toInt
      controllerContext.epochZkVersion = epochData._2.getVersion
      info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
    }
  }

  //移除该topic-partition管理员分配的备份节点集合相关内容,因为这个topic-partition目前的现状与管理员期待的相同
  def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
    if(controllerContext.partitionsBeingReassigned.get(topicAndPartition).isDefined) {
      // stop watching the ISR changes for this partition
      zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
        controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)//不再关注监听
    }
    // read the current list of reassigned partitions from zookeeper
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)//读取管理员分配topic-partition在哪些节点上做备份的配置
    // remove this partition from that list
    val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition //抛出该topic-partition,因为已经分配完成
    // write the new list to zookeeper
    ZkUtils.updatePartitionReassignmentData(zkClient, updatedPartitionsBeingReassigned.mapValues(_.newReplicas))//重新写zookeeper
    // update the cache. NO-OP if the partition's reassignment was never started
    controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
  }

  //为/brokers/topics/${topic}节点写入内容{partitions:{"1":[11,12,14],"2":[11,16,19]} }
  def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                         newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
    try {
      ///brokers/topics/${topic}
      val zkPath = ZkUtils.getTopicPath(topicAndPartition.topic)
      //ZkUtils.replicaAssignmentZkData的参数是Map[String, Seq[Int]] ,key是partition,value是该partition所在备份节点集合
      //为/brokers/topics/${topic}节点写入内容{partitions:{"1":[11,12,14],"2":[11,16,19]} }
      val jsonPartitionMap = ZkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => (e._1.partition.toString -> e._2)))
      ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case e: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
      case e2: Throwable => throw new KafkaException(e2.toString)
    }
  }

  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition],
                                                   isTriggeredByAutoRebalance : Boolean) {
    for(partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if(currentLeader == preferredReplica) {
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      } else {
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    if (!isTriggeredByAutoRebalance)
      ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath)
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   * @param brokers The brokers that the update metadata request should be sent to
   * 向节点集合中 发送指定partition集合的元数据信息
   */
  def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    brokerRequestBatch.newBatch()
    brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
    brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement)
  }

  /**
   * Removes a given partition replica from the ISR; if it is not the current
   * leader and there are sufficient remaining replicas in ISR.
   * @param topic topic
   * @param partition partition
   * @param replicaId replica Id
   * @return the new leaderAndIsr (with the replica removed if it was present),
   *         or None if leaderAndIsr is empty.
   */
  def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    //打印日志,移除某一个replicaId从ISR集合中xxx
    debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
      controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again 获取/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          if (leaderAndIsr.isr.contains(replicaId)) {//查找该partition对应的备份集合中是否有replicaId节点
            // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
            val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
            var newIsr = leaderAndIsr.isr.filter(b => b != replicaId)

            // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election
            // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can
            // eventually be restored as the leader.
            if (newIsr.isEmpty && !LogConfig.fromProps(config.props.props, AdminUtils.fetchTopicConfig(zkClient,
              topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              info("Retaining last ISR %d of partition %s since unclean leader election is disabled".format(replicaId, topicAndPartition))
              newIsr = leaderAndIsr.isr
            }

            val newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
              newIsr, leaderAndIsr.zkVersion + 1)
            // update the new leadership decision in zookeeper or retry
            val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partition,
              newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

            newLeaderAndIsr.zkVersion = newVersion
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            if (updateSucceeded)
              info("New leader and ISR for partition %s is %s".format(topicAndPartition, newLeaderAndIsr.toString()))
            updateSucceeded
          } else {//该节点不在备份集合中,则更新leader信息在内存中的映射
            warn("Cannot remove replica %d from ISR of partition %s since it is not in the ISR. Leader = %d ; ISR = %s"
                 .format(replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr))
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            true
          }
        case None =>
          warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   * @param topic topic
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   * 仅仅更新topic-partition对应的LeaderIsrAndControllerEpoch对象的leaderEpochm和zookeeper的version,不更新leader和isr集合
   */
  private def updateLeaderEpoch(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Updating leader epoch for partition %s.".format(topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again,获取/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) =>
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                                                 leaderAndIsr.isr, leaderAndIsr.zkVersion + 1)
          // update the new leadership decision in zookeeper or retry
          val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topic,
            partition, newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

          newLeaderAndIsr.zkVersion = newVersion
          finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
          if (updateSucceeded)
            info("Updated leader epoch for partition %s to %d".format(topicAndPartition, newLeaderAndIsr.leaderEpoch)) //更新zookeeper成功
          updateSucceeded
        case None =>
          throw new IllegalStateException(("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
            "This could mean we somehow tried to reassign a partition that doesn't exist").format(topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  class SessionExpirationListener() extends IZkStateListener with Logging {
    this.logIdent = "[SessionExpirationListener on " + config.brokerId + "], "
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
      info("ZK expired; shut down all controller components and try to re-elect")
      inLock(controllerContext.controllerLock) {
        onControllerResignation()
        controllerElector.elect
      }
    }
  }

  private def checkAndTriggerPartitionRebalance(): Unit = {
    if (isActive()) {
      trace("checking need to trigger partition rebalance")
      // get all the active brokers
      var preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicAndPartition, Seq[Int]]] = null
      inLock(controllerContext.controllerLock) {
        preferredReplicasForTopicsByBrokers =
          controllerContext.partitionReplicaAssignment.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p._1.topic)).groupBy {
            case(topicAndPartition, assignedReplicas) => assignedReplicas.head
          }
      }
      debug("preferred replicas by broker " + preferredReplicasForTopicsByBrokers)
      // for each broker, check if a preferred replica election needs to be triggered
      preferredReplicasForTopicsByBrokers.foreach {
        case(leaderBroker, topicAndPartitionsForBroker) => {
          var imbalanceRatio: Double = 0
          var topicsNotInPreferredReplica: Map[TopicAndPartition, Seq[Int]] = null
          inLock(controllerContext.controllerLock) {
            topicsNotInPreferredReplica =
              topicAndPartitionsForBroker.filter {
                case(topicPartition, replicas) => {
                  controllerContext.partitionLeadershipInfo.contains(topicPartition) &&
                  controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != leaderBroker
                }
              }
            debug("topics not in preferred replica " + topicsNotInPreferredReplica)
            val totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size
            val totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica.size
            imbalanceRatio = totalTopicPartitionsNotLedByBroker.toDouble / totalTopicPartitionsForBroker
            trace("leader imbalance ratio for broker %d is %f".format(leaderBroker, imbalanceRatio))
          }
          // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
          // that need to be on this broker
          if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
            topicsNotInPreferredReplica.foreach {
              case(topicPartition, replicas) => {
                inLock(controllerContext.controllerLock) {
                  // do this check only if the broker is live and there are no partitions being reassigned currently
                  // and preferred replica election is not in progress
                  if (controllerContext.liveBrokerIds.contains(leaderBroker) &&
                      controllerContext.partitionsBeingReassigned.size == 0 &&
                      controllerContext.partitionsUndergoingPreferredReplicaElection.size == 0 &&
                      !deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
                      controllerContext.allTopics.contains(topicPartition.topic)) {
                    onPreferredReplicaElection(Set(topicPartition), true)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed partition重新分配已经存在了
 * 2. New replicas are the same as existing replicas 配分的节点与现在存在的相同,不需要重新备份
 * 3. Any replica in the new set of replicas are dead 任何一个备份已经死掉了
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 * 以上三点任何一个满足了,都只是记录日志,并且从列表中移除该partition,不再进行分配
 * 为/admin/reassign_partitions节点注册事件PartitionsReassignedListener
 * 监听意义是 管理员为哪些topic-partition分配了备份节点集合
 */
class PartitionsReassignedListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PartitionsReassignedListener on " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
   * @throws Exception On any error.
   * 监听数据修改时间
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s"
      .format(dataPath, data))
    // 返回值是要将topic-partition分配到哪些节点去做备份 Map[TopicAndPartition, Seq[Int]]
    val partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString)
    val partitionsToBeReassigned = inLock(controllerContext.controllerLock) {
      partitionsReassignmentData.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
    }//过滤掉正在处理中的,即目前正在处理中的是不算的
    partitionsToBeReassigned.foreach { partitionToBeReassigned =>
      inLock(controllerContext.controllerLock) {
        if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(partitionToBeReassigned._1.topic)) {//说明当前topic已经被删除了
          error("Skipping reassignment of partition %s for topic %s since it is currently being deleted"
            .format(partitionToBeReassigned._1, partitionToBeReassigned._1.topic))
          controller.removePartitionFromReassignedPartitions(partitionToBeReassigned._1)//partitionToBeReassigned._1 表示TopicAndPartition
          //上面代码表示删除该topic在zookeeper中的配置,即不需要为该topic进行管理topic-partition-备份节点集合映射
        } else {//说明当前topic没有被删除
          val context = new ReassignedPartitionsContext(partitionToBeReassigned._2)
          controller.initiateReassignReplicasForTopicPartition(partitionToBeReassigned._1, context)//添加新的映射.表示正常处理中
        }
      }
    }
  }

  /**
   * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
   * @throws Exception
   *             On any error.
   * 监听数据删除事件            
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

/**
 * 一个监听器,当为topic-partition分配新的备份节点集合的时候,产生一个新的监听器
 * @param controller
 * @param topic
 * @param partition
 * @param reassignedReplicas 新的备份节点集合
 * /brokers/topics/${topic}/partitions/${partitionId}/state注册监听器
 */
class ReassignedPartitionsIsrChangeListener(controller: KafkaController, topic: String, partition: Int,
                                            reassignedReplicas: Set[Int]) //备份节点集合
  extends IZkDataListener with Logging {
  this.logIdent = "[ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions need to move leader to preferred replica
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    inLock(controllerContext.controllerLock) {
      debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data))
      val topicAndPartition = TopicAndPartition(topic, partition)
      try {
        // check if this partition is still being reassigned or not
        controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {//确定是否当前topic-partition正在被分配备份节点集合过程中
          case Some(reassignedPartitionContext) =>
            // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
            //获取/brokers/topics/ ${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
            val newLeaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition)
            newLeaderAndIsrOpt match {
              case Some(leaderAndIsr) => // check if new replicas have joined ISR
                //新的备份节点结婚 与 当前"isr": [同步副本组brokerId列表] 获取交集
                val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
                if(caughtUpReplicas == reassignedReplicas) {//交集与新的备份节点集合相同,说明匹配成功
                  // resume the partition reassignment process
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Resuming partition reassignment")
                  controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                }
                else {
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
                }
              case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
                .format(topicAndPartition, reassignedReplicas.mkString(",")))//说明还没有leader等信息内容
            }
          case None =>
        }
      } catch {
        case e: Throwable => error("Error while handling partition reassignment", e)
      }
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 * 管理员为一个topic-partition分配leader的时候监听器
 */
class PreferredReplicaElectionListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PreferredReplicaElectionListener on " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election %s"
            .format(dataPath, data.toString))
    inLock(controllerContext.controllerLock) {
      //解析新的内容,即哪些leader级别的topic-partition
      val partitionsForPreferredReplicaElection = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)
      if(controllerContext.partitionsUndergoingPreferredReplicaElection.size > 0)
        info("These partitions are already undergoing preferred replica election: %s"
          .format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
      val partitions = partitionsForPreferredReplicaElection -- controllerContext.partitionsUndergoingPreferredReplicaElection //新增的leader级别的topic-partition
      val partitionsForTopicsToBeDeleted = partitions.filter(p => controller.deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))//获取已经删除的topic
      if(partitionsForTopicsToBeDeleted.size > 0) {//打印日志
        error("Skipping preferred replica election for partitions %s since the respective topics are being deleted"
          .format(partitionsForTopicsToBeDeleted))
      }
      controller.onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)//抛出已经删除的topic
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

//重新分配topic-partition,参数newReplicas是重新分配的brokerId集合
case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: ReassignedPartitionsIsrChangeListener = null)

//该对象记录一个topic-partition-在broker上有备份,即参数replica就是备份的brokerId
case class PartitionAndReplica(topic: String, partition: Int, replica: Int) {
  override def toString(): String = {
    "[Topic=%s,Partition=%d,Replica=%d]".format(topic, partition, replica)
  }
}

/**
 * ISR:in-sync replicas
"controller_epoch": 表示kafka集群中的中央控制器选举次数,
"leader": 表示该partition选举leader的brokerId,
"version": 版本编号默认为1,
"leader_epoch": 该partition leader选举次数,
"isr": [同步副本组brokerId列表]
 */
case class LeaderIsrAndControllerEpoch(val leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString(): String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

object ControllerStats extends KafkaMetricsGroup {
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)
  val leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
