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


package kafka.api

import java.nio._
import kafka.utils._
import kafka.api.ApiUtils._
import kafka.cluster.Broker
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.network.{BoundedByteBufferSend, RequestChannel}
import kafka.common.ErrorMapping
import kafka.network.RequestChannel.Response
import collection.Set

//isr:in-sync reassigned replica同步重新分配replica 
object LeaderAndIsr {
  val initialLeaderEpoch: Int = 0 //初始化该partition的leader的选举次数
  val initialZKVersion: Int = 0 //初始化该partition的leader的zookeeper版本号
  val NoLeader = -1//表示没有leader
  val LeaderDuringDelete = -2//表示该leader正在删除中
}

/**
"controller_epoch": 表示kafka集群中的中央控制器选举次数,
"leader": 表示该partition选举leader的brokerId,
"version": 版本编号默认为1,
"leader_epoch": 该partition leader选举次数,
"isr": [同步副本组brokerId列表]
 */
case class LeaderAndIsr(var leader: Int, var leaderEpoch: Int, var isr: List[Int], var zkVersion: Int) {
  def this(leader: Int, isr: List[Int]) = this(leader, LeaderAndIsr.initialLeaderEpoch, isr, LeaderAndIsr.initialZKVersion)

  override def toString(): String = {
    Json.encode(Map("leader" -> leader, "leader_epoch" -> leaderEpoch, "isr" -> isr))
  }
}

//描述一个partition的leader的详细信息和备份节点集合信息
object PartitionStateInfo {
  def readFrom(buffer: ByteBuffer): PartitionStateInfo = {
    val controllerEpoch = buffer.getInt
    val leader = buffer.getInt
    val leaderEpoch = buffer.getInt
    val isrSize = buffer.getInt
    val isr = for(i <- 0 until isrSize) yield buffer.getInt
    val zkVersion = buffer.getInt
    val replicationFactor = buffer.getInt
    val replicas = for(i <- 0 until replicationFactor) yield buffer.getInt
    PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, leaderEpoch, isr.toList, zkVersion), controllerEpoch),
                       replicas.toSet)
  }
}

/**
 * 描述当前partition的详细信息和备份节点集合信息
 * @param leaderIsrAndControllerEpoch 该partition的leader对象详细信息
 * @param allReplicas 该partition的备份节点集合
 */
case class PartitionStateInfo(val leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                              val allReplicas: Set[Int]) {
  def replicationFactor = allReplicas.size //备份节点数量

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(leaderIsrAndControllerEpoch.controllerEpoch) //当前controller的选举次数
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leader) //当前leader节点信息
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch) //leader的选举次数
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.isr.size)//同步节点数量
    leaderIsrAndControllerEpoch.leaderAndIsr.isr.foreach(buffer.putInt(_))//同步的节点集合
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion) //当前leader节点在zookeeper上的版本号
    buffer.putInt(replicationFactor)//备份节点数量
    allReplicas.foreach(buffer.putInt(_))//备份节点集合
  }

  def sizeInBytes(): Int = {
    val size =
      4 /* epoch of the controller that elected the leader */ +
      4 /* leader broker id */ +
      4 /* leader epoch */ +
      4 /* number of replicas in isr */ +
      4 * leaderIsrAndControllerEpoch.leaderAndIsr.isr.size /* replicas in isr */ +
      4 /* zk version */ +
      4 /* replication factor */ +
      allReplicas.size * 4
    size
  }
  
  override def toString(): String = {
    val partitionStateInfo = new StringBuilder
    partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch.toString)
    partitionStateInfo.append(",ReplicationFactor:" + replicationFactor + ")")
    partitionStateInfo.append(",AllReplicas:" + allReplicas.mkString(",") + ")")
    partitionStateInfo.toString()
  }
}

object LeaderAndIsrRequest {
  val CurrentVersion = 0.shortValue
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def readFrom(buffer: ByteBuffer): LeaderAndIsrRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val controllerId = buffer.getInt
    val controllerEpoch = buffer.getInt
    val partitionStateInfosCount = buffer.getInt
    val partitionStateInfos = new collection.mutable.HashMap[(String, Int), PartitionStateInfo]

    for(i <- 0 until partitionStateInfosCount){
      val topic = readShortString(buffer)
      val partition = buffer.getInt
      val partitionStateInfo = PartitionStateInfo.readFrom(buffer)

      partitionStateInfos.put((topic, partition), partitionStateInfo)
    }

    val leadersCount = buffer.getInt
    var leaders = Set[Broker]()
    for (i <- 0 until leadersCount)
      leaders += Broker.readFrom(buffer)

    new LeaderAndIsrRequest(versionId, correlationId, clientId, controllerId, controllerEpoch, partitionStateInfos.toMap, leaders)
  }
}

//参数partitionStateInfos,key是topic-partition组成的元组,value是该partition所对应的备份信息
case class LeaderAndIsrRequest (versionId: Short,
                                correlationId: Int,//关联请求的唯一ID
                                clientId: String,//客户端名称,一般是host-port等信息,意义不是很大
                                controllerId: Int,//发送该请求的是哪个controller节点,此时controller节点就一个.通过这个参数可以知道controller节点是哪个
                                controllerEpoch: Int,//此时controller选举的次数
                                partitionStateInfos: Map[(String, Int), PartitionStateInfo],//该接收节点上此时每一个topic-partition对应的leader详细信息和备份节点集合
                                leaders: Set[Broker])//这些topic-partition所在的leader节点集合
    extends RequestOrResponse(Some(RequestKeys.LeaderAndIsrKey)) {

  def this(partitionStateInfos: Map[(String, Int), PartitionStateInfo], leaders: Set[Broker], controllerId: Int,
           controllerEpoch: Int, correlationId: Int, clientId: String) = {
    this(LeaderAndIsrRequest.CurrentVersion, correlationId, clientId,
         controllerId, controllerEpoch, partitionStateInfos, leaders)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(controllerId)
    buffer.putInt(controllerEpoch)
    buffer.putInt(partitionStateInfos.size) //多少个topic-partition
    for((key, value) <- partitionStateInfos){
      writeShortString(buffer, key._1) //topic
      buffer.putInt(key._2) //partition
      value.writeTo(buffer) //具体的leader详细信息和备份节点集合信息
    }
    buffer.putInt(leaders.size) //多少个leader节点
    leaders.foreach(_.writeTo(buffer))//每一个leader节点内容
  }

  def sizeInBytes(): Int = {
    var size =
      2 /* version id */ +
      4 /* correlation id */ + 
      (2 + clientId.length) /* client id */ +
      4 /* controller id */ +
      4 /* controller epoch */ +
      4 /* number of partitions */
    for((key, value) <- partitionStateInfos)
      size += (2 + key._1.length) /* topic */ + 4 /* partition */ + value.sizeInBytes /* partition state info */
    size += 4 /* number of leader brokers */
    for(broker <- leaders)
      size += broker.sizeInBytes /* broker info */
    size
  }

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val responseMap = partitionStateInfos.map {
      case (topicAndPartition, partitionAndState) => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    val errorResponse = LeaderAndIsrResponse(correlationId, responseMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val leaderAndIsrRequest = new StringBuilder
    leaderAndIsrRequest.append("Name:" + this.getClass.getSimpleName)
    leaderAndIsrRequest.append(";Version:" + versionId)
    leaderAndIsrRequest.append(";Controller:" + controllerId)
    leaderAndIsrRequest.append(";ControllerEpoch:" + controllerEpoch)
    leaderAndIsrRequest.append(";CorrelationId:" + correlationId)
    leaderAndIsrRequest.append(";ClientId:" + clientId)
    leaderAndIsrRequest.append(";Leaders:" + leaders.mkString(","))
    if(details)
      leaderAndIsrRequest.append(";PartitionState:" + partitionStateInfos.mkString(","))
    leaderAndIsrRequest.toString()
  }
}