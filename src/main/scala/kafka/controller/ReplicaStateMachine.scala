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
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.{TopicAndPartition, StateChangeFailedException}
import kafka.utils.{ZkUtils, ReplicationUtils, Logging}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.log4j.Logger
import kafka.controller.Callbacks._
import kafka.utils.Utils._

/**
 * 该类表示一个partition的备份replicas的所有状态,以及状态之间的转换关系
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 *                        a.controller创建一个replica,在为partition重新分配备份的时候,这个状态下的replica备份对象
 *                        b.仅仅能够变成follower,即不能作为一个partition的leader
 *                        c.该状态前仅仅是由NonExistentReplica状态变更回来
 *                        
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 *                        a.一旦一个备份对象已经开始了,为partition分配了信息,他就是该状态
 *                        b.该状态可以成为leader,也可以成为follower
 *                        c.该状态可以由NewReplica OnlineReplica or OfflineReplica三者转变过来
 *                        
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 *                        a.如果一个备份对象死了,他就是OfflineReplica状态,这种情况发生在该replica所在的broker挂了,所以该备份对象也就等于死了
 *                        b.该状态由NewReplica, OnlineReplica转变过来
 *                        
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 *                            a.如果要对该备份对象删除,则要先将其设置为ReplicaDeletionStarted状态
 *                            b.该状态由OfflineReplica转变过来
 * 
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                               moved to this state. Valid previous state is ReplicaDeletionStarted
 *                               a.如果一个备份对象在删除请求的回复中,没有error错误码,则认为是删除成功,即ReplicaDeletionSuccessful状态
 *                               b.该状态由ReplicaDeletionStarted转变过来
 *                               
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 *                               a.如果一个备份对象删除失败,会移动到ReplicaDeletionIneligible状态
 *                               b.该状态由ReplicaDeletionStarted转变过来
 * 
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 *                        a.如果一个备份对象成功删除,就将其设置为NonExistentReplica状态
 *                        b.该状态由ReplicaDeletionSuccessful转变过来
 *                        
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId//controller所在的broker节点ID
  private val zkClient = controllerContext.zkClient
  //存储每一个topic-partition-replica组合的的状态映射关系
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  private val brokerChangeListener = new BrokerChangeListener()//对/brokers/ids节点进行监听
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "


  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   */
  def startup() {
    // initialize replica state 初始化每一个topic--partition--replicaId对应的状态
    initializeReplicaState()
    // set started flag
    hasStarted.set(true)
    // move all Online replicas to Online 移动所有在活着的broker节点上的备份对象状态为OnlineReplica
    //controllerContext.allLiveReplicas()返回PartitionAndReplica集合,即  该对象记录一个topic-partition-在broker上有备份,即参数replica就是备份的brokerId
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  // register ZK listeners of the replica state machine 注册监听
  def registerListeners() {
    // register broker change listener
    registerBrokerChangeListener()
  }

  // de-register ZK listeners of the replica state machine 解除注册监听
  def deregisterListeners() {
    // de-register broker change listener
    deregisterBrokerChangeListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // reset replica state
    replicaState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped replica state machine")
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   * 将参数1中的集合每一个状态都设置为参数2
   * 作用:更新PartitionAndReplica集合的状态,并且有回调
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    if(replicas.size > 0) {
      //日志说明,更新replicas集合的状态,目标状态为targetState
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()//校验
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
      }catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * 状态机的流程,确保每一次状态的转换都是合法的转换
   * 有效的状态转换规则如下:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache

   * @param partitionAndReplica The replica for which the state transition is invoked 当前要去操作的topic-partition-replica对象
   * @param targetState The end state that the replica should be moved to 最终需要变成的状态
   * 将当前topic-partition-replica对象的状态移动到targetState状态上去,然后执行回调方法
   */
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks) {
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                                            "to %s failed because replica state machine has not started")
                                              .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
    //获取当前partitionAndReplica对应的状态,默认状态是NonExistentReplica
    val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    try {
      //获取该topic-partition对象,对应的partition的备份的ID集合
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      targetState match {
        case NewReplica =>
          //校验 当前的PartitionAndReplica对象所对应的备份状态,应该在参数fromStates集合中,并且最终目的是要把当前状态移动到targetState状态上
          assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition 开始为当前的partition产生一个follower的replica对象
          //获取/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象,从而获取到该topic-partition对应的leader是哪个节点
          val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) //发现自己就是leader,这个是不允许的
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
              //将topic-partition的follow节点集合brokerIds添加到leader集合内,即添加leader和follow的对应关系
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch,
                                                                  replicaAssignment)
            case None => // new leader request will be sent to this replica when one gets elected 说明该partition目前没有leader对象,则将该节点作为leader
          }
          replicaState.put(partitionAndReplica, NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                            targetState))
        case ReplicaDeletionStarted =>
          assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          // send stop replica command
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionIneligible =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionSuccessful =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case NonExistentReplica =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
          // remove this replica from the assigned replicas list for its partition
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          replicaState.remove(partitionAndReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case OnlineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          replicaState(partitionAndReplica) match {
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              if(!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                        .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                                targetState))
            case _ =>
              // check if the leader for this partition ever existed
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  replicaState.put(partitionAndReplica, OnlineReplica)
                  stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                  // started a log for that partition and does not have a high watermark value for this partition
              }
          }
          replicaState.put(partitionAndReplica, OnlineReplica)
        case OfflineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          // send stop replica command to the replica so that it stops fetching from the leader
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // As an optimization, the controller removes dead replicas from the ISR
          val leaderAndIsrIsEmpty: Boolean =
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              case Some(currLeaderIsrAndControllerEpoch) =>
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request to all the remaining alive replicas of the partition.
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) {
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    }
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                      .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                    false
                  case None =>
                    true
                }
              case None =>
                true
            }
          if (leaderAndIsrIsEmpty)
            throw new StateChangeFailedException(
              "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
              .format(replicaId, topicAndPartition))
      }
    }
    catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] from %s to %s failed"
                                  .format(controllerId, controller.epoch, replicaId, topic, partition, currState, targetState), t)
    }
  }

  //true表示该topic所有的replicas都删除成功
  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)//通过topic获取备份集合,set[PartitionAndReplica]
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap//转化成每一个PartitionAndReplica,对应的备份对象状态 映射关系 Map[PartitionAndReplica,ReplicaState]
    debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
    //r指代PartitionAndReplica,ReplicaState,因此r._2为ReplicaState
    //因此如果有任意一个ReplicaState不是ReplicaDeletionSuccessful,则说明topic所有的备份对象有至少一个没成功删除,因此返回false
    replicaStatesForTopic.foldLeft(true)((deletionState, r) => deletionState && r._2 == ReplicaDeletionSuccessful)
  }

  //true表示topic中存在至少一个备份对象是已经准备删除中
  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)//通过topic获取备份集合
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap //转化成每一个PartitionAndReplica,对应的备份对象状态 映射关系 Map[PartitionAndReplica,ReplicaState]
    //r指代PartitionAndReplica,ReplicaState,因此r._2为ReplicaState
    //因此如果有任意一个ReplicaState是ReplicaDeletionStarted,则说明topic中存在至少一个备份对象是正在准备删除状态中
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  //返回属于该topic,并且也是参数state状态的所有PartitionAndReplica对象集合
  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    //遍历每一个PartitionAndReplica对象,过滤条件是topic和ReplicaState对象,返回过滤后的PartitionAndReplica集合
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  //返回是否存在该topic在state状态下的备份对象,true表示存在
  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
  }

  //过滤该topic参数对应的Replica备份对象集合中,属于删除状态的对象集合被返回
  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set(ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)//删除状态集合
    //过滤属于该topic,并且状态是删除状态集合中的Replica备份对象集合
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  //校验 当前的PartitionAndReplica对象所对应的备份状态,应该在参数fromStates集合中,并且最终目的是要把当前状态移动到targetState状态上
  //属于断言操作,即参数1PartitionAndReplica所对应的备份状态,一定是在参数2集合中即可
  private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    //断言当前状态一定在fromStates集合内
    //如果不在则打印日志,当前处理的topic-partition-replica对象是xxx,应该在xxxx集合中,在移动到xxx状态前,当前状态是xxx
    assert(fromStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  //注册监听
  private def registerBrokerChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  //解除注册监听
  private def deregisterBrokerChangeListener() = {
    zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   * 初始化每一个topic--partition--replicaId对应的状态
   */
  private def initializeReplicaState() {
    //key是topic-partition对象,value是该partition的备份的ID集合,进行循环每一个
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition
      //循环该topic-partition上面所有的备份节点ID
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
        //如果该replaca备份节点,是活跃节点,则将该PartitionAndReplica状态设置为OnlineReplica,否则设置为ReplicaDeletionIneligible,即备份不可用
        controllerContext.liveBrokerIds.contains(replicaId) match {
          case true => replicaState.put(partitionAndReplica, OnlineReplica)
          case false =>
            // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
            // This is required during controller failover since during controller failover a broker can go down,
            // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
            replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  //查询在该brokerId节点上有topic-partition备份的 topic-partition集合
  //目的是当brokerId节点挂了,则找到该节点上所有的topic-partition备份集合
  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    //key是topic-partition对象,value是该partition的备份的ID集合
    //在上面定义的集合中过滤,_2获取的是partition的备份的ID集合,只要该集合有brokerId,则返回该topic-partition对象集合
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
   * 对/brokers/ids节点监听,当节点有变化的时候调用该函数
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.mkString(",")))
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          ControllerStats.leaderElectionTimer.time {
            try {
              val curBrokerIds = currentBrokerList.map(_.toInt).toSet//获取当前节点集合
              val newBrokerIds = curBrokerIds -- controllerContext.liveOrShuttingDownBrokerIds//获取新增的节点集合
              //确定有意义的新增节点集合
              val newBrokerInfo = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _))
              val newBrokers = newBrokerInfo.filter(_.isDefined).map(_.get)
              //计算已经死掉的节点集合
              val deadBrokerIds = controllerContext.liveOrShuttingDownBrokerIds -- curBrokerIds
              
              //重新生成全局的正常的节点集合
              controllerContext.liveBrokers = curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
              
              //记录日志,说明新增哪些节点、死掉哪些节点、当前所有的正在使用的节点集合信息
              info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                .format(newBrokerIds.mkString(","), deadBrokerIds.mkString(","), controllerContext.liveBrokerIds.mkString(",")))
                
              //开启和删除节点集合信息
              newBrokers.foreach(controllerContext.controllerChannelManager.addBroker(_))
              deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker(_))
              if(newBrokerIds.size > 0)
                controller.onBrokerStartup(newBrokerIds.toSeq)
              if(deadBrokerIds.size > 0)
                controller.onBrokerFailure(deadBrokerIds.toSeq)
            } catch {
              case e: Throwable => error("Error while handling broker changes", e)
            }
          }
        }
      }
    }
  }
}

//备份对象的状态
sealed trait ReplicaState { def state: Byte }
case object NewReplica extends ReplicaState { val state: Byte = 1 }//表示一个备份对象Replica刚刚被创建,并且用于follow节点使用
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }//表示一个备份对象Replica已经正式使用,并且可以用于leader,也可以用于follow节点使用
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }//表示一个备份对象Replica所在节点挂了,因此该节点暂时不可用
case object ReplicaDeletionStarted extends ReplicaState { val state: Byte = 4}//表示一个备份对象Replica开始删除
case object ReplicaDeletionSuccessful extends ReplicaState { val state: Byte = 5}//表示一个备份对象Replica删除成功
case object ReplicaDeletionIneligible extends ReplicaState { val state: Byte = 6}//表示一个备份对象Replica删除失败
case object NonExistentReplica extends ReplicaState { val state: Byte = 7 }//表示一个备份对象Replica不存在
