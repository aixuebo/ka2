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
import collection.JavaConversions
import collection.mutable.Buffer
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.utils.{Logging, ZkUtils, ReplicationUtils}
import org.I0Itec.zkclient.{IZkDataListener, IZkChildListener}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.log4j.Logger
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.Utils._

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 *                          a.说明该partiton从来没有被创建过,或者曾经创建过,但是后来被删除了
 *                          b.该状态只能从OfflinePartition状态转移过来,前提还得是以前存在过,后来被删除的情况下
 *                          
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 *                          a.当partition创建完成后,就是NewPartition状态.该状态下应该有replicas备份对象被分配给该partition了,但是没有leader和isr被分配
 *                          b.该状态只能从NonExistentPartition状态流转过来
 *                          
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 *                          a.一旦partition有leader被选举成功,他就是OnlinePartition状态
 *                          b.从NewPartition/OfflinePartition状态流转到OnlinePartition
 *                          
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 *                          a.一旦leader选举成功后,leader挂了,则partition归属到OfflinePartition状态,等待继续选举一个leader
 *                          b.从NewPartition/OnlinePartition状态转移到OfflinePartition状态
 */
class PartitionStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId //controller所在broker节点Id
  private val zkClient = controllerContext.zkClient
  
  //每一个topic-partiton对应的PartitionState状态对象 映射关系
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  
  private val topicChangeListener = new TopicChangeListener()///brokers/topics节点监听
  private val deleteTopicsListener = new DeleteTopicsListener()///admin/delete_topics节点监听
  
  //key是topic,value是该topic上建立的监听,即/brokers/topics/${topic}节点上建立监听
  private val addPartitionsListener: mutable.Map[String, AddPartitionsListener] = mutable.Map.empty
  
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Partition state machine on Controller " + controllerId + "]: "

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
   */
  def startup() {
    // initialize partition state
    initializePartitionState() //初始化每一个partition的状态
    // set started flag
    hasStarted.set(true)
    // try to move partitions to online state
    triggerOnlinePartitionStateChange()

    //打印每一个partition的状态
    info("Started partition state machine with initial state -> " + partitionState.toString())
  }

  // register topic and partition change listeners
  def registerListeners() {
    registerTopicChangeListener()
    if(controller.config.deleteTopicEnable)
      registerDeleteTopicListener()
  }

  // de-register topic and partition change listeners
  def deregisterListeners() {
    deregisterTopicChangeListener()
    addPartitionsListener.foreach {
      case (topic, listener) =>
        zkClient.unsubscribeDataChanges(ZkUtils.getTopicPath(topic), listener)
    }
    addPartitionsListener.clear()
    if(controller.config.deleteTopicEnable)
      deregisterDeleteTopicListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // clear partition state
    partitionState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   * 针对不是删除的topic进行以下逻辑处理,将OfflinePartition或者NewPartition的状态转换成OnlinePartition状态
   */
  def triggerOnlinePartitionStateChange() {
    try {
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
      // that belong to topics to be deleted
      //循环每一个partition,如果该partition对应的topic暂时不是要删除状态时,则进行处理逻辑
      for((topicAndPartition, partitionState) <- partitionState
          if(!controller.deleteTopicManager.isTopicQueuedUpForDeletion(topicAndPartition.topic))) {//针对不是删除的topic进行以下逻辑处理,将OfflinePartition或者NewPartition的状态转换成OnlinePartition状态
            if(partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
              handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                                (new CallbackBuilder).build)
          }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
    } catch {
      case e: Throwable => error("Error while moving some partitions to the online state", e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
    }
  }

  //过滤查找符合该状态的TopicAndPartition集合
  def partitionsInState(state: PartitionState): Set[TopicAndPartition] = {
    // Map[TopicAndPartition, PartitionState],因此_2是PartitionState
    partitionState.filter(p => p._2 == state).keySet
  }

  /**
   * This API is invoked by the partition change zookeeper listener
   * @param partitions   The list of partitions that need to be transitioned to the target state
   * @param targetState  The state that the partitions should be moved to
   * 状态更改
   */
  def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                         leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks)
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
    }catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param topic       The topic of the partition for which the state transition is invoked
   * @param partition   The partition for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to 最终更改后的状态目标
   * @param leaderSelector  为partition选择leader、同步节点集合、备份节点集合的实现类
   * 给topic-partition更改状态
   */
  private def handleStateChange(topic: String, partition: Int, targetState: PartitionState,
                                leaderSelector: PartitionLeaderSelector,
                                callbacks: Callbacks) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                                            "the partition state machine has not started")
                                              .format(controllerId, controller.epoch, topicAndPartition, targetState))
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition) //获取当前partition的状态
    try {
      targetState match {
        case NewPartition =>
          //以前不存在该partition,现在要产生一个新的partition
          // pre: partition did not exist before this
          assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
          assignReplicasToPartitions(topic, partition) //让controller知道该topic-partition在哪些节点上有备份
          partitionState.put(topicAndPartition, NewPartition) //设置映射
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",") //获取当前该partition有哪些备份节点集合
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                            assignedReplicas))
          // post: partition has been assigned replicas
        case OnlinePartition =>
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
          partitionState(topicAndPartition) match {//获取当前状态
            case NewPartition =>
              // initialize leader and isr path for new partition
              //从备份节点集合中产生leader节点和同步节点集合,然后向zookeeper中/brokers/topics/${topic}/partitions/${partitionId}/state里面写入leader内容
              initializeLeaderAndIsrForPartition(topicAndPartition)
            case OfflinePartition =>
              electLeaderForPartition(topic, partition, leaderSelector)
            case OnlinePartition => // invoked when the leader needs to be re-elected
              electLeaderForPartition(topic, partition, leaderSelector)
            case _ => // should never come here since illegal previous states are checked above 其他状态忽略处理
          }
          //设置映射
          partitionState.put(topicAndPartition, OnlinePartition)
          //获取leader,打印日志
          val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to %s with leader %d"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState, leader))
           // post: partition has a leader
        case OfflinePartition =>
          //什么也没有做,就是存储映射
          // pre: partition should be in New or Online state
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OfflinePartition)
          // should be called when the leader for a partition is no longer alive
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, OfflinePartition)
          // post: partition has no alive leader
        case NonExistentPartition =>
          //必须从无效的partition状态转变过来,设置映射即可
          // pre: partition should be in Offline state
          assertValidPreviousStates(topicAndPartition, List(OfflinePartition), NonExistentPartition)
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, NonExistentPartition)
          // post: partition state is deleted from all brokers and zookeeper
      }
    } catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   * 初始化每一个topic-partition的状态,默认是NewPartition
   * 如果该topic-partition的leader在活着的broker节点中存在,则设置为OnlinePartition
   * 如果该topic-partition的leader不在活着的broker节点中存在,则设置为OfflinePartition
   */
  private def initializePartitionState() {
    //循环key是topic-partition对象,value是该partition的备份的ID集合
    for((topicPartition, replicaAssignment) <- controllerContext.partitionReplicaAssignment) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      //校验给定topic-partition对应的leader节点
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          //如果对应的leader节点存在,并且是活跃的broker节点,因此设置为OnlinePartition状态
          controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader) match {
            case true => // leader is alive
              partitionState.put(topicPartition, OnlinePartition)//如果该topic-partition的leader在活着的broker节点中存在,则设置为OnlinePartition
            case false =>
              partitionState.put(topicPartition, OfflinePartition)//如果该topic-partition的leader不在活着的broker节点中存在,则设置为OfflinePartition
          }
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  //断言,校验参数1对应的状态,一定在参数2的集合中才允许继续访问
  private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    if(!fromStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  /**
   * Invoked on the NonExistentPartition->NewPartition state transition to update the controller's cache with the
   * partition's replica assignment.
   * 当状态由NonExistentPartition->NewPartition转变中被执行该方法,让controller知道该partition对应哪些备份节点
   * @param topic     The topic of the partition whose replica assignment is to be cached
   * @param partition The partition whose replica assignment is to be cached
   */
  private def assignReplicasToPartitions(topic: String, partition: Int) {
    val assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition) //读取/brokers/topics/${topic}节点,获取该topic-partition有哪些备份节点
    controllerContext.partitionReplicaAssignment += TopicAndPartition(topic, partition) -> assignedReplicas
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, it's leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
   * 当状态从NewPartition->OnlinePartition过程变化的时候,调用该方法,
   * 此时partition是新的,尚未有leader和同步节点集合,仅仅有备份节点集合,
   * 一旦状态是OnlinePartition的时候,就要有leader和同步节点集合,并且也不能回退到new状态了,
   * 从此他只能被转换成OfflinePartition下线状态
   * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
   */
  private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) {
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)//获取该topic-partition对应的备份节点集合
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))//过滤仅仅保留活着的brokerId集合
    liveAssignedReplicas.size match {
      case 0 =>
        //说明没有活着的备份节点集合了,因此抛异常,这种情况基本不太可能存在
        val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                       "live brokers are [%s]. No assigned replica is alive.")
                         .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg)
      case _ =>
        debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
        // make the first replica in the list of assigned replicas, the leader
        //第一个备份节点做为leader节点
        val leader = liveAssignedReplicas.head
        //构造leader对象,将所有备份节点集合作为同步节点集合使用
        val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
          controller.epoch)
        debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
        try {
          //向/brokers/topics/${topic}/partitions/${partitionId}/state里面写入leader内容
          ZkUtils.createPersistentPath(controllerContext.zkClient,
            ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            ZkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
          // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
          // took over and initialized this partition. This can happen if the current controller went into a long
          // GC pause
          //向controller添加leader信息
          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
          //发送leader请求
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
            topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
        } catch {
          case e: ZkNodeExistsException =>
            // read the controller epoch
            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic,
              topicAndPartition.partition).get
            val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                           "exists with value %s and controller epoch %d")
                             .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
            stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
            throw new StateChangeFailedException(failMsg)
        }
    }
  }

  /**
   * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
   * It invokes the leader election API to elect a leader for the input offline partition
   * 在状态由OfflinePartition->OnlinePartition,或者OnlinePartition->OnlinePartition转变的时候被调用,
   * 重新选择一个leader
   * @param topic               The topic of the offline partition
   * @param partition           The offline partition
   * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.) 指定leader的选择器
   */
  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    // handle leader election for the partitions whose leader is no longer alive
    stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s"
                              .format(controllerId, controller.epoch, topicAndPartition))
    try {
      var zookeeperPathUpdateSucceeded: Boolean = false //zookeeper内容是否更新成功
      var newLeaderAndIsr: LeaderAndIsr = null
      var replicasForThisPartition: Seq[Int] = Seq.empty[Int]//备份节点集合
      while(!zookeeperPathUpdateSucceeded) {//只要没更新成功,就不断更新
        val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition) //获取当前partition的leader对象
        val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr //当前leader的选举次数
        val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch //当前controller的选举次数
        if (controllerEpoch > controller.epoch) {//不允许比controller自身的还大,抛异常
          val failMsg = ("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                         "already written by another controller. This probably means that the current controller %d went through " +
                         "a soft failure and another controller was elected with epoch %d.")
                           .format(topic, partition, controllerId, controllerEpoch)
          stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
          throw new StateChangeFailedException(failMsg)
        }
        // elect new leader or throw exception 重新设置leader对象和备份节点集合
        val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
        //更新/brokers/topics/${topic}/partitions/${partitionId}/state下的json内容信息
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partition,
          leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion)
        //更新新内容
        newLeaderAndIsr = leaderAndIsr
        newLeaderAndIsr.zkVersion = newVersion
        zookeeperPathUpdateSucceeded = updateSucceeded //更新成功
        replicasForThisPartition = replicas
      }

      //新的leader对象
      val newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
      // update the leader cache 更新controller中leader对象
      controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
      stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s"
                                .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))
      //获取当前备份节点集合
      val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
      // store new leader and isr info in cache
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
        newLeaderIsrAndControllerEpoch, replicas)
    } catch {
      case lenne: LeaderElectionNotNeededException => // swallow
      case nroe: NoReplicaOnlineException => throw nroe
      case sce: Throwable =>
        val failMsg = "encountered error while electing leader for partition %s due to: %s.".format(topicAndPartition, sce.getMessage)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg, sce)
    }
    debug("After leader election, leader cache is updated to %s".format(controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
  }

  //添加/brokers/topics节点监听
  private def registerTopicChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicChangeListener)
  }

  //解除/brokers/topics节点监听
  private def deregisterTopicChangeListener() = {
    zkClient.unsubscribeChildChanges(ZkUtils.BrokerTopicsPath, topicChangeListener)
  }

  //为topic添加/brokers/topics/${topic}节点上监听
  def registerPartitionChangeListener(topic: String) = {
    addPartitionsListener.put(topic, new AddPartitionsListener(topic))
    zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), addPartitionsListener(topic))
  }

  //为topic解除/brokers/topics/${topic}节点上监听
  def deregisterPartitionChangeListener(topic: String) = {
    zkClient.unsubscribeDataChanges(ZkUtils.getTopicPath(topic), addPartitionsListener(topic))
    addPartitionsListener.remove(topic)
  }

  //注册/admin/delete_topics节点监听
  private def registerDeleteTopicListener() = {
    zkClient.subscribeChildChanges(ZkUtils.DeleteTopicsPath, deleteTopicsListener)
  }

  //解除/admin/delete_topics节点监听
  private def deregisterDeleteTopicListener() = {
    zkClient.unsubscribeChildChanges(ZkUtils.DeleteTopicsPath, deleteTopicsListener)
  }

  //读取/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
  private def getLeaderIsrAndEpochOrThrowException(topic: String, partition: Int): LeaderIsrAndControllerEpoch = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition) match {
      case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
      case None =>
        val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state"
                        .format(topicAndPartition, partitionState(topicAndPartition))
        throw new StateChangeFailedException(failMsg)
    }
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
   * 针对/brokers/topics节点进行监听
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          try {
            //获取现在拥有的全部topic
            val currentChildren = {
              import JavaConversions._
              debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
              (children: Buffer[String]).toSet
            }
            //做差值,获取新添加的topic集合
            val newTopics = currentChildren -- controllerContext.allTopics
            //做差值,获取已经删除的topic
            val deletedTopics = controllerContext.allTopics -- currentChildren
            //更新现在已经获取的最新topic集合
            controllerContext.allTopics = currentChildren

            //为新增的topic,读取/brokers/topics/${topic},获取新增的topic都有哪些partition,以及每一个partition分配哪些备份节点集合,返回值 Map[TopicAndPartition, Seq[Int]]
            val addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, newTopics.toSeq)
            
            //过滤已经不存在的topic-partition映射关系,返回值还是key是topic-partition对象,value是该partition的备份的ID集合
            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
              !deletedTopics.contains(p._1.topic))
             
            //将新添加的topic-partition在哪些broker节点上信息追加到全局中
            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
            //记录日志,新topic……已经删除的topic……,以及新添加的topic-partition分布在哪些节点上
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))

            //创建新的topic
            if(newTopics.size > 0)
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
          } catch {
            case e: Throwable => error("Error while handling new topic", e )
          }
        }
      }
    }
  }

  /**
   * Delete topics includes the following operations -
   * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
   * 2. If there are topics to be deleted, it signals the delete topic thread
   * 
   * /admin/delete_topics节点监听
   */
  class DeleteTopicsListener() extends IZkChildListener with Logging {
    this.logIdent = "[DeleteTopicsListener on " + controller.config.brokerId + "]: "
    val zkClient = controllerContext.zkClient

    /**
     * Invoked when a topic is being deleted
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        //获取将要删除的topic集合
        var topicsToBeDeleted = {
          import JavaConversions._
          (children: Buffer[String]).toSet
        }
        //打印要删除哪些topic集合
        debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
        //查找要删除,但是已经不存在的topic集合
        val nonExistentTopics = topicsToBeDeleted.filter(t => !controllerContext.allTopics.contains(t))
        if(nonExistentTopics.size > 0) {//如果存在,则真正去删除zookeeper节点,/admin/delete_topics/${topic},因为已经topic不存在了
          warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
          nonExistentTopics.foreach(topic => ZkUtils.deletePathRecursive(zkClient, ZkUtils.getDeleteTopicPath(topic)))
        }
        //过滤获取剩余的要删除的topic集合
        topicsToBeDeleted --= nonExistentTopics
        if(topicsToBeDeleted.size > 0) {
          info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
          // mark topic ineligible for deletion if other state changes are in progress
          topicsToBeDeleted.foreach { topic =>
            //读取zookeeper中/admin/preferred_replica_election节点存储的topic-partition集合,这些集合意义是管理员设置的leader级别的topic-partition,判断是否包含该topic
            val preferredReplicaElectionInProgress =
              controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic).contains(topic)//是否包含该topic

            //读取zookeeper中/admin/reassign_partitions节点内容--解析成管理员分配的topic-partition-在哪些节点集合中备份,判断是否包含该topic
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)//是否包含该topic

            if(preferredReplicaElectionInProgress || partitionReassignmentInProgress)
              controller.deleteTopicManager.markTopicIneligibleForDeletion(Set(topic)) //将其topic设置为暂时不合法
          }
          // add topic to deletion list 真正加入删除topic队列
          controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      }
    }

    /**
     * @throws Exception
     * On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
    }
  }

  ///brokers/topics/${topic}节点上建立监听,每一个topic对应一个该监听对象
  //监听partition数量以及备份节点集合的变化
  class AddPartitionsListener(topic: String) extends IZkDataListener with Logging {

    this.logIdent = "[AddPartitionsListener on " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleDataChange(dataPath : String, data: Object) {
      inLock(controllerContext.controllerLock) {
        try {
          info("Add Partition triggered " + data.toString + " for path " + dataPath)
          //读取/brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
          //返回值Map[TopicAndPartition, Seq[Int]]
          val partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
          
          //过滤不存在的topic-partition,即新增的partition,返回值Map[TopicAndPartition, Seq[Int] ]
          val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
            !controllerContext.partitionReplicaAssignment.contains(p._1))
            
            //校验该topic是否已经准备删除,如果是准备删除,则要打印日志,说明这些节点要删除gaugetopic数据,因为是删除的topic,也就没必要对新增加的partition进行处理了
          if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
            error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                  .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
          else {
            //通知controller新增加了partition
            if (partitionsToBeAdded.size > 0) {
              info("New partitions to be added %s".format(partitionsToBeAdded))
              controller.onNewPartitionCreation(partitionsToBeAdded.keySet.toSet)
            }
          }
        } catch {
          case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e )
        }
      }
    }

    @throws(classOf[Exception])
    def handleDataDeleted(parentPath : String) {
      // this is not implemented for partition change
    }
  }
}

//参见最上面的备注,描述各个状态含义和转换流程
sealed trait PartitionState { def state: Byte }
case object NewPartition extends PartitionState { val state: Byte = 0 }
case object OnlinePartition extends PartitionState { val state: Byte = 1 }
case object OfflinePartition extends PartitionState { val state: Byte = 2 }
case object NonExistentPartition extends PartitionState { val state: Byte = 3 }
