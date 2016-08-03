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

import collection.mutable
import kafka.utils.{ShutdownableThread, Logging, ZkUtils}
import kafka.utils.Utils._
import collection.Set
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.api.{StopReplicaResponse, RequestOrResponse}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller has a background thread that handles topic deletion. The purpose of having this background thread
 *    is to accommodate the TTL feature, when we have it. This thread is signaled whenever deletion for a topic needs to
 *    be started or resumed. Currently, a topic's deletion can be started only by the onPartitionDeletion callback on the
 *    controller. In the future, it can be triggered based on the configured TTL for the topic. A topic will be ineligible
 *    for deletion in the following scenarios -
 *    3.1 broker hosting one of the replicas for that topic goes down
 *    3.2 partition reassignment for partitions of that topic is in progress
 *    3.3 preferred replica election for partitions of that topic is in progress
 *    (though this is not strictly required since it holds the controller lock for the entire duration from start to end)
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 preferred replica election for partitions of that topic completes
 *    4.3 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted (Replica enters TopicDeletionStarted phase when the onPartitionDeletion callback is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse)
 *    5.3 TopicDeletionFailed. (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.)
 * 6. The delete topic thread marks a topic successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state and it starts the topic deletion teardown mode where it deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 * @param controller
 * @param initialTopicsToBeDeleted The topics that are queued up for deletion in zookeeper at the time of controller failover
 *         controller故障转移时候,从zookeeper中读取的要删除的topic队列集合
 * @param initialTopicsIneligibleForDeletion The topics ineligible for deletion due to any of the conditions mentioned in #3 above
 *         不合格的topic集合,这些topic要去被删除,这些不合格的topic是上面3个条件任意一个条件触发时候,都认为他是不合格的topic
 */
class TopicDeletionManager(controller: KafkaController,
                           initialTopicsToBeDeleted: Set[String] = Set.empty,
                           initialTopicsIneligibleForDeletion: Set[String] = Set.empty) extends Logging {
  this.logIdent = "[Topic Deletion Manager " + controller.config.brokerId + "], "
  val controllerContext = controller.controllerContext
  val partitionStateMachine = controller.partitionStateMachine
  val replicaStateMachine = controller.replicaStateMachine
  
  //存储要删除的topic集合
  val topicsToBeDeleted: mutable.Set[String] = mutable.Set.empty[String] ++ initialTopicsToBeDeleted
  //根据要删除的topic集合,查找这些topic所对应的所有TopicAndPartition集合,即要被删除的TopicAndPartition集合,返回值Set[TopicAndPartition]
  val partitionsToBeDeleted: mutable.Set[TopicAndPartition] = topicsToBeDeleted.flatMap(controllerContext.partitionsForTopic)
  
  val deleteLock = new ReentrantLock()
  
  val topicsIneligibleForDeletion: mutable.Set[String] = mutable.Set.empty[String] ++
    (initialTopicsIneligibleForDeletion & initialTopicsToBeDeleted)//不合格的与要删除的topic 交集
  val deleteTopicsCond = deleteLock.newCondition()

  val deleteTopicStateChanged: AtomicBoolean = new AtomicBoolean(false)//true表示有topic要被去删除,需要DeleteTopicsThread线程执行
  var deleteTopicsThread: DeleteTopicsThread = null//真正去删除topic的线程
  val isDeleteTopicEnabled = controller.config.deleteTopicEnable//只有该值为true时,才表示允许进行topic删除操作

  /**
   * Invoked at the end of new controller initiation
   */
  def start() {
    //开启线程去执行删除topic操作
    if (isDeleteTopicEnabled) {
      deleteTopicsThread = new DeleteTopicsThread()
      if (topicsToBeDeleted.size > 0)
        deleteTopicStateChanged.set(true)//只有存在要删除的topic集合的时候,才设置为true
      deleteTopicsThread.start()
    }
  }

  /**
   * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared.
   */
  def shutdown() {
    // Only allow one shutdown to go through
    if (isDeleteTopicEnabled && deleteTopicsThread.initiateShutdown()) {
      // Resume the topic deletion so it doesn't block on the condition
      resumeTopicDeletionThread()
      // Await delete topic thread to exit
      deleteTopicsThread.awaitShutdown()
      topicsToBeDeleted.clear()
      partitionsToBeDeleted.clear()
      topicsIneligibleForDeletion.clear()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * 
   * 监听/admin/delete_topics子节点,即等待删除的topic,将该topic调用该方法,进行删除该topic的partition和备份信息,一旦删除成功完成,则移除该/admin/delete_topics中topic节点
   * 例如:该topic的所有的partitions的所有的replicas都被删除后,则认为是成功删除topic,则移除/admin/delete_topics/topic节点
   * @param topics Topics that should be deleted 参数表示要删除的topic集合
   * 将要删除的topic集合添加到topicsToBeDeleted队列中
   * 将要删除的topic集合对应的Partition集合添加到partitionsToBeDeleted队列中
   */
  def enqueueTopicsForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) {
      topicsToBeDeleted ++= topics
      partitionsToBeDeleted ++= topics.flatMap(controllerContext.partitionsForTopic)
      resumeTopicDeletionThread()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * 3. Preferred replica election completes. Any partitions belonging to topics queued up for deletion finished
   *    preferred replica election
   * @param topics Topics for which deletion can be resumed
   * 将一些topic不去删除
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty) {
    if(isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & topicsToBeDeleted//获取集合交集,即不去删除哪些集合
      if(topicsToResumeDeletion.size > 0) {
        topicsIneligibleForDeletion --= topicsToResumeDeletion //说明这些topic又变成合法的topic了
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice. The delete topic thread is notified so it can retry topic deletion
   * if it has received a response for all replicas of a topic to be deleted
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    if(isDeleteTopicEnabled) {
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if(replicasThatFailedToDelete.size > 0) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug("Deletion failed for replicas %s. Halting deletion for topics %s"
          .format(replicasThatFailedToDelete.mkString(","), topics))
        controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete, ReplicaDeletionIneligible)
        markTopicIneligibleForDeletion(topics)
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * 3. preferred replica election in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = topicsToBeDeleted & topics
      topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if(newTopicsToHaltDeletion.size > 0)
        info("Halted deletion of topics %s".format(newTopicsToHaltDeletion.mkString(",")))
    }
  }

  def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  def isTopicDeletionInProgress(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)
    } else
      false
  }

  def isPartitionToBeDeleted(topicAndPartition: TopicAndPartition) = {
    if(isDeleteTopicEnabled) {
      partitionsToBeDeleted.contains(topicAndPartition)
    } else
      false
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      topicsToBeDeleted.contains(topic)
    } else
      false
  }

  /**
   * Invoked by the delete-topic-thread to wait until events that either trigger, restart or halt topic deletion occur.
   * controllerLock should be acquired before invoking this API
   */
  private def awaitTopicDeletionNotification() {
    inLock(deleteLock) {
      while(deleteTopicsThread.isRunning.get() && !deleteTopicStateChanged.compareAndSet(true, false)) {//将条件状态由true变成false,等待下次通知变成true为止
        debug("Waiting for signal to start or continue topic deletion")
        deleteTopicsCond.await()//一旦状态更改完毕,就等待下一次信号接收
      }
    }
  }

  /**
   * Signals the delete-topic-thread to process topic deletion
   * 去让线程可以删除topic
   */
  private def resumeTopicDeletionThread() {
    deleteTopicStateChanged.set(true)//条件设置为true
    inLock(deleteLock) {
      deleteTopicsCond.signal()
    }
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. The delete
   * topic thread is notified so it can tear down the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  private def completeReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug("Deletion successfully completed for replicas %s".format(successfullyDeletedReplicas.mkString(",")))
    controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas, ReplicaDeletionSuccessful)
    resumeTopicDeletionThread()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic) && (!isTopicDeletionInProgress(topic) && !isTopicIneligibleForDeletion(topic))
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   *@param topic Topic for which deletion should be retried
   */
  private def markTopicForDeletionRetry(topic: String) {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionIneligible)
    info("Retrying delete topic for topic %s since replicas %s were not successfully deleted"
      .format(topic, failedReplicas.mkString(",")))
    controller.replicaStateMachine.handleStateChanges(failedReplicas, OfflineReplica)
  }

  //该方法指代topic所有的partition的replica备份都已经删除完成,因此该topic也要删除掉
  private def completeDeleteTopic(topic: String) {
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    partitionStateMachine.deregisterPartitionChangeListener(topic)
    val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica)
    val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
    // move respective partition to OfflinePartition and NonExistentPartition state
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition)
    topicsToBeDeleted -= topic
    partitionsToBeDeleted.retain(_.topic != topic)
    controllerContext.zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
    controllerContext.zkClient.deleteRecursive(ZkUtils.getTopicConfigPath(topic))
    controllerContext.zkClient.delete(ZkUtils.getDeleteTopicPath(topic))
    controllerContext.removeTopic(topic)
  }

  /**
   * This callback is invoked by the DeleteTopics thread with the list of topics to be deleted
   * It invokes the delete partition callback for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   */
  private def onTopicDeletion(topics: Set[String]) {
    info("Topic deletion callback for %s".format(topics.mkString(",")))
    // send update metadata so that brokers stop serving data for topics to be deleted
    val partitions = topics.flatMap(controllerContext.partitionsForTopic)
    controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
    val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
    topics.foreach { topic =>
      onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).map(_._1).toSet)
    }
  }

  /**
   * Invoked by the onPartitionDeletion callback. It is the 2nd step of topic deletion, the first being sending
   * UpdateMetadata requests to all brokers to start rejecting requests for deleted topics. As part of starting deletion,
   * the topics are added to the in progress list. As long as a topic is in the in progress list, deletion for that topic
   * is never retried. A topic is removed from the in progress list when
   * 1. Either the topic is successfully deleted OR
   * 2. No replica for the topic is in ReplicaDeletionStarted state and at least one replica is in ReplicaDeletionIneligible state
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * As part of starting deletion, all replicas are moved to the ReplicaDeletionStarted state where the controller sends
   * the replicas a StopReplicaRequest (delete=true)
   * This callback does the following things -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all alive replicas to ReplicaDeletionStarted state so they can be deleted successfully
   *@param replicasForTopicsToBeDeleted
   */
  private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
    replicasForTopicsToBeDeleted.groupBy(_.topic).foreach { case(topic, replicas) =>
      var aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic.equals(topic))
      val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
      val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
      val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
      // move dead replicas directly to failed state
      replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionIneligible)
      // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
      replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica)
      debug("Deletion started for replicas %s".format(replicasForDeletionRetry.mkString(",")))
      controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
        new Callbacks.CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build)
      if(deadReplicasForTopic.size > 0) {
        debug("Dead Replicas (%s) found for topic %s".format(deadReplicasForTopic.mkString(","), topic))
        markTopicIneligibleForDeletion(Set(topic))
      }
    }
  }

  /**
   * This callback is invoked by the delete topic callback with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Send UpdateMetadataRequest to all live brokers (that are not shutting down) for partitions that are being
   *    deleted. The brokers start rejecting all client requests with UnknownTopicOrPartitionException
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicAndPartition]) {
    info("Partition deletion callback for %s".format(partitionsToBeDeleted.mkString(",")))
    val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
    startReplicaDeletion(replicasPerPartition)
  }

  private def deleteTopicStopReplicaCallback(stopReplicaResponseObj: RequestOrResponse, replicaId: Int) {
    val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
    debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
    val partitionsInError = if(stopReplicaResponse.errorCode != ErrorMapping.NoError) {
      stopReplicaResponse.responseMap.keySet
    } else
      stopReplicaResponse.responseMap.filter(p => p._2 != ErrorMapping.NoError).map(_._1).toSet
    val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
    inLock(controllerContext.controllerLock) {
      // move all the failed replicas to ReplicaDeletionIneligible
      failReplicaDeletion(replicasInError)
      if(replicasInError.size != stopReplicaResponse.responseMap.size) {
        // some replicas could have been successfully deleted
        val deletedReplicas = stopReplicaResponse.responseMap.keySet -- partitionsInError
        completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
      }
    }
  }

  //线程,专门去删除topic
  class DeleteTopicsThread() extends ShutdownableThread(name = "delete-topics-thread-" + controller.config.brokerId, isInterruptible = false) {
    val zkClient = controllerContext.zkClient
    override def doWork() {//不断的执行该方法
      awaitTopicDeletionNotification() //等待下一次信号接收

      if (!isRunning.get)
        return

      inLock(controllerContext.controllerLock) {
        
        //copy等待删除的topic
        val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted

        //打印将要删除的topic集合
        if(!topicsQueuedForDeletion.isEmpty)
          info("Handling deletion for topics " + topicsQueuedForDeletion.mkString(","))

          //一个一个topic进行删除
        topicsQueuedForDeletion.foreach { topic =>
        // if all replicas are marked as deleted successfully, then topic deletion is done表示该topic所有的replicas都删除成功
          if(controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {
            // clear up all state for this topic from controller cache and zookeeper
            completeDeleteTopic(topic)
            info("Deletion of topic %s successfully completed".format(topic))
          } else {//说明该topic还有没有删除的备份
            if(controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {//true表示topic中存在至少一个备份对象是已经准备删除中
              // ignore since topic deletion is in progress 因为该topic已经在删除进程中了,因此忽略
              val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
              val replicaIds = replicasInDeletionStartedState.map(_.replica)
              val partitions = replicasInDeletionStartedState.map(r => TopicAndPartition(r.topic, r.partition))
              info("Deletion for replicas %s for partition %s of topic %s in progress".format(replicaIds.mkString(","),
                partitions.mkString(","), topic))
            } else {//说明该topic还没有进入删除过程中
              // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
              // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
              // or there is at least one failed replica (which means topic deletion should be retried).
              if(controller.replicaStateMachine.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {///返回是否存在该topic在state状态下的备份对象,true表示存在
                // mark topic for deletion retry
                markTopicForDeletionRetry(topic)
              }
            }
          }
          // Try delete topic if it is eligible for deletion.
          if(isTopicEligibleForDeletion(topic)) {
            info("Deletion of topic %s (re)started".format(topic))
            // topic deletion will be kicked off
            onTopicDeletion(Set(topic))
          } else if(isTopicIneligibleForDeletion(topic)) {
            info("Not retrying deletion of topic %s at this time since it is marked ineligible for deletion".format(topic))
          }
        }
      }
    }
  }
}
