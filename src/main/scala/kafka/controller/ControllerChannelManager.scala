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

import kafka.network.{Receive, BlockingChannel}
import kafka.utils.{Utils, Logging, ShutdownableThread}
import collection.mutable.HashMap
import kafka.cluster.Broker
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}
import kafka.server.KafkaConfig
import collection.mutable
import kafka.api._
import org.apache.log4j.Logger
import scala.Some
import kafka.common.TopicAndPartition
import kafka.api.RequestOrResponse
import collection.Set

class ControllerChannelManager (private val controllerContext: ControllerContext, config: KafkaConfig) extends Logging {
  //key是管理与哪个broker节点建立,value:与该节点连接的各种数据的组装对象
  private val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  //创建与每个节点的链接,以及各种内存映射关系
  controllerContext.liveBrokers.foreach(addNewBroker(_))

  //打开与每一个节点的线程
  def startup() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  //关闭与每一个节点的线程
  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => removeExistingBroker(brokerState._1))
    }
  }

  //向brokerId节点发送RequestOrResponse请求.请求后回调函数为callback
  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) {
    brokerLock synchronized {
      //获取与该brokerId节点的请求队列对象,向请求队列中添加该请求和回调函数
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put((request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }

  //单独添加一个节点,与该节点进行连接
  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerId)
    }
  }

  private def addNewBroker(broker: Broker) {
    //为每一个Broker创建一个队列,队列缓存的是一组元组
    val messageQueue = new LinkedBlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)](config.controllerMessageQueueSize)
    //尝试连接该Broker
    debug("Controller %d trying to connect to broker %d".format(config.brokerId,broker.id))
    val channel = new BlockingChannel(broker.host, broker.port,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      config.controllerSocketTimeoutMs)
    
    //线程,按照顺序从队列中获取一个请求,发送到toBroker服务器,接收response数据,调用队列的回调函数
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, broker, messageQueue, channel)
    requestThread.setDaemon(false)
    
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(channel, broker, messageQueue, requestThread))
  }

  //关闭与该节点的链接,情况请求队列,注意 如果队列里面有数据,也不会被请求了
  private def removeExistingBroker(brokerId: Int) {
    try {
      brokerStateInfo(brokerId).channel.disconnect()
      brokerStateInfo(brokerId).messageQueue.clear()
      brokerStateInfo(brokerId).requestSendThread.shutdown()
      brokerStateInfo.remove(brokerId)
    }catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  //打开该节点的线程
  private def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

//线程,按照顺序从队列中获取一个请求,发送到toBroker服务器,接收response数据,调用队列的回调函数
class RequestSendThread(val controllerId: Int,//当前controller的broker节点ID
                        val controllerContext: ControllerContext,//上下文对象
                        val toBroker: Broker,//连接到哪个broker,该对象是服务器端
                        val queue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],//存储队列
                        val channel: BlockingChannel)//controllerId向toBroker建立连接后的流
  extends ShutdownableThread("Controller-%d-to-broker-%d-send-thread".format(controllerId, toBroker.id)) {
  
  private val lock = new Object()
  private val stateChangeLogger = KafkaController.stateChangeLogger
  //真正去连接broker
  connectToBroker(toBroker, channel)

  //不断被线程的run方法调用
  override def doWork(): Unit = {
    val queueItem = queue.take()//取出一组元组
    val request = queueItem._1
    val callback = queueItem._2
    var receive: Receive = null
    try {
      lock synchronized {
        var isSendSuccessful = false
        while(isRunning.get() && !isSendSuccessful) {
          // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
          // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
          try {
            channel.send(request)
            receive = channel.receive()
            isSendSuccessful = true
          } catch {
            //如果出现异常,则重新连接broker,并且重新发送信息
            case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
              warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
                "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                request.toString, toBroker.toString()), e)
              channel.disconnect()
              connectToBroker(toBroker, channel)
              isSendSuccessful = false
              // backoff before retrying the connection and send
              Utils.swallow(Thread.sleep(300))
          }
        }
        
        //根据请求不同,返回不同的response对象
        var response: RequestOrResponse = null
        request.requestId.get match {
          case RequestKeys.LeaderAndIsrKey =>
            response = LeaderAndIsrResponse.readFrom(receive.buffer)
          case RequestKeys.StopReplicaKey =>
            response = StopReplicaResponse.readFrom(receive.buffer)
          case RequestKeys.UpdateMetadataKey =>
            response = UpdateMetadataResponse.readFrom(receive.buffer)
        }
        stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
                                  .format(controllerId, controllerContext.epoch, response.toString, toBroker.toString))

        //如果设置了回调函数,则调用回调函数,传入response返回值
        if(callback != null) {
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, toBroker.toString()), e)
        // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.
        channel.disconnect()
    }
  }

  //真正去连接broker
  private def connectToBroker(broker: Broker, channel: BlockingChannel) {
    try {
      channel.connect()
      info("Controller %d connected to %s for sending state change requests".format(controllerId, broker.toString()))
    } catch {
      case e: Throwable => {
        channel.disconnect()
        error("Controller %d's connection to broker %s was unsuccessful".format(controllerId, broker.toString()), e)
      }
    }
  }
}

//该类与ReplicaStateMachine类、PartitionStateMachine类关系较大
class ControllerBrokerRequestBatch(controller: KafkaController) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  val clientId: String = controller.clientId
  //key是节点ID,value是一个Map,key是元组topic-partition组成,value是该partition对应的PartitionStateInfo对象
  val leaderAndIsrRequestMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), PartitionStateInfo]]
  //key是节点ID,value是该节点上等待删除的备份集合,
  //StopReplicaRequestInfo表示要删除的一个partition备份信息,包含partition-topic-brokerId信息,是否删除该partition,以及回调函数
  val stopReplicaRequestMap = new mutable.HashMap[Int, Seq[StopReplicaRequestInfo]]
  val updateMetadataRequestMap = new mutable.HashMap[Int, mutable.HashMap[TopicAndPartition, PartitionStateInfo]]
  private val stateChangeLogger = KafkaController.stateChangeLogger

  def newBatch() {
    // raise error if the previous batch is not empty 因为要开启一个批处理,因此以前的数据是不应该存在的,因此校验如果存在数据,则抛异常
    if(leaderAndIsrRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if(stopReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if(updateMetadataRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()))
  }

  /**
   * 该方法作用流程:
   * a.为参数1 brokerIds集合都是参数2和3组成的topic-partition上的备份节点的follow集合
   * b.参数leaderIsrAndControllerEpoch表示该topic-partition上leader等信息
   * c.参数replicas,表示已知该topic-partition上备份在哪些broker节点集合中
   * d.参数callback表示执行完调用回调函数
   * 
   * 主要作用:
   * 将topic-partition的follow节点集合brokerIds添加到leader集合内,即添加leader和follow的对应关系
   */
  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], callback: (RequestOrResponse) => Unit = null) {
    val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, partition)

    brokerIds.filter(b => b >= 0).foreach {
      brokerId =>
        leaderAndIsrRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[(String, Int), PartitionStateInfo])
        //key是节点ID,value是一个Map,key是元组topic-partition组成,value是该partition对应的PartitionStateInfo对象
        leaderAndIsrRequestMap(brokerId).put((topic, partition),
          PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(topicAndPartition))
  }

  //删除topic-partition在brokerIds节点集合上的信息,删除后调用callback回调函数
  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (RequestOrResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      val v = stopReplicaRequestMap(brokerId)
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: RequestOrResponse) => { callback(r, brokerId) })
          //说明 (r: RequestOrResponse) => { callback(r, brokerId) }  表示参数r是RequestOrResponse类型的,调用callback方法,传入r和该brokerId
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                         callback: (RequestOrResponse) => Unit = null) {
    def updateMetadataRequestMapFor(partition: TopicAndPartition, beingDeleted: Boolean) {
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition).toSet //该partition所在节点集合
          val partitionStateInfo = if (beingDeleted) {
            val leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr)
            PartitionStateInfo(LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas)
          } else {
            PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          }
          brokerIds.filter(b => b >= 0).foreach { brokerId =>
            updateMetadataRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[TopicAndPartition, PartitionStateInfo])
            updateMetadataRequestMap(brokerId).put(partition, partitionStateInfo)
          }
        case None =>
          info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
      }
    }

    val filteredPartitions = {
      val givenPartitions = if (partitions.isEmpty)
        controllerContext.partitionLeadershipInfo.keySet
      else
        partitions
      if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty)
        givenPartitions
      else
        givenPartitions -- controller.deleteTopicManager.partitionsToBeDeleted
    }
    filteredPartitions.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = false))
    controller.deleteTopicManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = true))
  }

  def sendRequestsToBrokers(controllerEpoch: Int, correlationId: Int) {
    leaderAndIsrRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos = m._2.toMap
      val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
      val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id))
      val leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos, leaders, controllerId, controllerEpoch, correlationId, clientId)
      for (p <- partitionStateInfos) {
        val typeOfRequest = if (broker == p._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
        stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s with correlationId %d to broker %d " +
                                 "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                 p._2.leaderIsrAndControllerEpoch, correlationId, broker,
                                                                 p._1._1, p._1._2))
      }
      controller.sendRequest(broker, leaderAndIsrRequest, null)
    }
    leaderAndIsrRequestMap.clear()
    updateMetadataRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos = m._2.toMap
      val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, correlationId, clientId,
        partitionStateInfos, controllerContext.liveOrShuttingDownBrokers)
      partitionStateInfos.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s with " +
        "correlationId %d to broker %d for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
        correlationId, broker, p._1)))
      controller.sendRequest(broker, updateMetadataRequest, null)
    }
    updateMetadataRequestMap.clear()
    stopReplicaRequestMap foreach { case(broker, replicaInfoList) =>
      val stopReplicaWithDelete = replicaInfoList.filter(p => p.deletePartition == true).map(i => i.replica).toSet
      val stopReplicaWithoutDelete = replicaInfoList.filter(p => p.deletePartition == false).map(i => i.replica).toSet
      debug("The stop replica request (delete = true) sent to broker %d is %s"
        .format(broker, stopReplicaWithDelete.mkString(",")))
      debug("The stop replica request (delete = false) sent to broker %d is %s"
        .format(broker, stopReplicaWithoutDelete.mkString(",")))
      replicaInfoList.foreach { r =>
        val stopReplicaRequest = new StopReplicaRequest(r.deletePartition,
          Set(TopicAndPartition(r.replica.topic, r.replica.partition)), controllerId, controllerEpoch, correlationId)
        controller.sendRequest(broker, stopReplicaRequest, r.callback)
      }
    }
    stopReplicaRequestMap.clear()
  }
}

case class ControllerBrokerStateInfo(channel: BlockingChannel,//与服务器broker建立连接的socket链接对象
                                     broker: Broker,//服务器broker节点
                                     messageQueue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],//存储发往服务器broker节点的数据请求队列
                                     requestSendThread: RequestSendThread)//真正从messageQueue队列中获取数据,执行发送请求,接收返回值,处理回调函数的线程

//表示要删除的一个partition备份信息,包含partition-topic-brokerId信息,是否删除该partition,以及回调函数
case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: (RequestOrResponse) => Unit = null)

class Callbacks private (var leaderAndIsrResponseCallback:(RequestOrResponse) => Unit = null,
                         var updateMetadataResponseCallback:(RequestOrResponse) => Unit = null,
                         var stopReplicaResponseCallback:(RequestOrResponse, Int) => Unit = null)

object Callbacks {
  class CallbackBuilder {
    //参数是RequestOrResponse,返回值是Unit,默认为null
    var leaderAndIsrResponseCbk:(RequestOrResponse) => Unit = null
    var updateMetadataResponseCbk:(RequestOrResponse) => Unit = null
    var stopReplicaResponseCbk:(RequestOrResponse, Int) => Unit = null

    def leaderAndIsrCallback(cbk: (RequestOrResponse) => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk
      this
    }

    def updateMetadataCallback(cbk: (RequestOrResponse) => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk
      this
    }

    def stopReplicaCallback(cbk: (RequestOrResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
    }
  }
}
