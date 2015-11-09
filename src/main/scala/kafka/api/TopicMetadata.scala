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

import kafka.cluster.Broker
import java.nio.ByteBuffer
import kafka.api.ApiUtils._
import kafka.utils.Logging
import kafka.common._
import org.apache.kafka.common.utils.Utils._

//记录一个topic拥有一个PartitionMetadata集合的信息
object TopicMetadata {
  
  val NoLeaderNodeId = -1//该topic下所有的partition中是没有leader的,即没有设置replicas复制功能

  //从ByteBuffer中反序列化TopicMetadata对象
  def readFrom(buffer: ByteBuffer, brokers: Map[Int, Broker]): TopicMetadata = {
    val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
    val topic = readShortString(buffer)
    val numPartitions = readIntInRange(buffer, "number of partitions", (0, Int.MaxValue))
    val partitionsMetadata: Array[PartitionMetadata] = new Array[PartitionMetadata](numPartitions)
    for(i <- 0 until numPartitions) {
      val partitionMetadata = PartitionMetadata.readFrom(buffer, brokers)
      partitionsMetadata(partitionMetadata.partitionId) = partitionMetadata
    }
    new TopicMetadata(topic, partitionsMetadata, errorCode)
  }
}

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMapping.NoError) extends Logging {
  
  //该topic对应的PartitionMetadata集合需要多少个字节大小请求
  def sizeInBytes: Int = {
    2 /* error code */ + 
    shortStringLength(topic) + //topic字符串所需要字节长度
    4 + // partitionsMetadata集合大小,即size,用int表示
    partitionsMetadata.map(_.sizeInBytes).sum /* size and partition data array 计算集合中每一个PartitionMetadata元素所需要字节大小,然后汇总求和*/
  }

  //将序列化信息写入到ByteBuffer中
  def writeTo(buffer: ByteBuffer) {
    /* error code */
    buffer.putShort(errorCode)
    /* topic */
    writeShortString(buffer, topic)
    /* number of partitions */
    buffer.putInt(partitionsMetadata.size)
    partitionsMetadata.foreach(m => m.writeTo(buffer))
  }

  /**
   * 格式化json输出
   * 
   */
  override def toString(): String = {
    val topicMetadataInfo = new StringBuilder
    topicMetadataInfo.append("{TopicMetadata for topic %s -> ".format(topic))
    errorCode match {
      case ErrorMapping.NoError =>//没有异常
        partitionsMetadata.foreach { partitionMetadata => //循环每一个partition
          partitionMetadata.errorCode match {
            case ErrorMapping.NoError =>//没有异常,则输出该topic-partition对应的信息
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                partitionMetadata.partitionId, partitionMetadata.toString()))
            case ErrorMapping.ReplicaNotAvailableCode =>//该异常可忽略
              // this error message means some replica other than the leader is not available. The consumer
              // doesn't care about non leader replicas, so ignore this
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                partitionMetadata.partitionId, partitionMetadata.toString()))
            case _ =>//表示topic-partition不可用异常
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is not available due to %s".format(topic,
                partitionMetadata.partitionId, ErrorMapping.exceptionFor(partitionMetadata.errorCode).getClass.getName))
          }
        }
      case _ =>//有异常,输出该topic请求中出现的异常className,有可能说明该topic没有对应的partition信息
        topicMetadataInfo.append("\nNo partition metadata for topic %s due to %s".format(topic,
                                 ErrorMapping.exceptionFor(errorCode).getClass.getName))
    }
    topicMetadataInfo.append("}")
    topicMetadataInfo.toString()
  }
}

//关于partition信息的元数据对象,一个topic可以拥有多个该对象
object PartitionMetadata {

  //从ByteBuffer中反序列化成一个PartitionMetadata对象
  def readFrom(buffer: ByteBuffer, brokers: Map[Int, Broker]): PartitionMetadata = {
    val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
    val partitionId = readIntInRange(buffer, "partition id", (0, Int.MaxValue)) /* partition id */
    val leaderId = buffer.getInt
    val leader = brokers.get(leaderId)//返回该leaderId对应的Broker对象

    /* list of all replicas 读取该partitionId的备份数量 */
    val numReplicas = readIntInRange(buffer, "number of all replicas", (0, Int.MaxValue))
    val replicaIds = (0 until numReplicas).map(_ => buffer.getInt)//获取该备份的Broker节点ID集合
    val replicas = replicaIds.map(brokers)//将Broker节点ID集合映射成Broker集合

    /* list of in-sync replicas */
    val numIsr = readIntInRange(buffer, "number of in-sync replicas", (0, Int.MaxValue))
    val isrIds = (0 until numIsr).map(_ => buffer.getInt)
    val isr = isrIds.map(brokers)

    new PartitionMetadata(partitionId, leader, replicas, isr, errorCode)
  }
}

case class PartitionMetadata(partitionId: Int, 
                             val leader: Option[Broker], 
                             replicas: Seq[Broker], 
                             isr: Seq[Broker] = Seq.empty,
                             errorCode: Short = ErrorMapping.NoError) extends Logging {
  def sizeInBytes: Int = {
    2 /* error code */ + 
    4 /* partition id */ + 
    4 /* leader */ + //如果没有leader,则设置为-1
    4 + 4 * replicas.size /* replica array */ + //第一个4表示replicas的size,每一个replicas对应的Broker的ID要写入到里面,每一个id对应4个字节的int
    4 + 4 * isr.size /* isr array */ //第一个4表示isr的size,每一个isr对应的Broker的ID要写入到里面,每一个id对应4个字节的int
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(errorCode)
    buffer.putInt(partitionId)

    /* leader */
    val leaderId = if(leader.isDefined) leader.get.id else TopicMetadata.NoLeaderNodeId
    buffer.putInt(leaderId)

    /* number of replicas */
    buffer.putInt(replicas.size)
    replicas.foreach(r => buffer.putInt(r.id))

    /* number of in-sync replicas */
    buffer.putInt(isr.size)
    isr.foreach(r => buffer.putInt(r.id))
  }

  override def toString(): String = {
    val partitionMetadataString = new StringBuilder
    partitionMetadataString.append("\tpartition " + partitionId)
    partitionMetadataString.append("\tleader: " + (if(leader.isDefined) formatBroker(leader.get) else "none"))
    partitionMetadataString.append("\treplicas: " + replicas.map(formatBroker).mkString(","))//将集合中每一个Broker进行格式化,格式化的结果用逗号拆分,组成一个字符串
    partitionMetadataString.append("\tisr: " + isr.map(formatBroker).mkString(","))
    partitionMetadataString.append("\tisUnderReplicated: %s".format(if(isr.size < replicas.size) "true" else "false"))
    partitionMetadataString.toString()
  }

  //将Broker进行格式化
  private def formatBroker(broker: Broker) = broker.id + " (" + formatAddress(broker.host, broker.port) + ")"
}


