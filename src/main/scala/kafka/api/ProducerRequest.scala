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
import kafka.message._
import kafka.api.ApiUtils._
import kafka.common._
import kafka.network.RequestChannel.Response
import kafka.network.{RequestChannel, BoundedByteBufferSend}

object ProducerRequest {
  val CurrentVersion = 0.shortValue

  def readFrom(buffer: ByteBuffer): ProducerRequest = {
    val versionId: Short = buffer.getShort
    val correlationId: Int = buffer.getInt
    val clientId: String = readShortString(buffer)
    val requiredAcks: Short = buffer.getShort
    val ackTimeoutMs: Int = buffer.getInt
    //build the topic structure
    val topicCount = buffer.getInt
    //表示该生产者请求,按照topic-partition分组,每组都是一个ByteBufferMessageSet对象
    val partitionDataPairs = (1 to topicCount).flatMap(_ => {
      // process topic
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partition = buffer.getInt
        val messageSetSize = buffer.getInt
        val messageSetBuffer = new Array[Byte](messageSetSize)
        buffer.get(messageSetBuffer,0,messageSetSize)
        (TopicAndPartition(topic, partition), new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)))
      })
    })

    ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, collection.mutable.Map(partitionDataPairs:_*))
  }
}

/**
 * @param data collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet],表示该生产者请求,按照topic-partition分组,每组都是一个ByteBufferMessageSet对象
 */
case class ProducerRequest(versionId: Short = ProducerRequest.CurrentVersion,
                           correlationId: Int,//是该客户端发送的第几个数据
                           clientId: String,//表示哪个客户端传递过来的数据
                           requiredAcks: Short,
                           ackTimeoutMs: Int,
                           data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet])//要向哪些topic-partition发送message集合
    extends RequestOrResponse(Some(RequestKeys.ProduceKey)) {

  /**
   * Partitions the data into a map of maps (one for each topic).
   * 将Map[TopicAndPartition, ByteBufferMessageSet] 按照topic分组,生成Map[topic,Map[TopicAndPartition, ByteBufferMessageSet]]
   */
  private lazy val dataGroupedByTopic = data.groupBy(_._1.topic)
  
  //映射成Map[TopicAndPartition,ByteBufferMessageSet.sizeInBytes]
  val topicPartitionMessageSizeMap = data.map(r => r._1 -> r._2.sizeInBytes).toMap

  def this(correlationId: Int,
           clientId: String,
           requiredAcks: Short,
           ackTimeoutMs: Int,
           data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) =
    this(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data)

    //与上面的readFrom方法相反,将对象写入到ByteBuffer中
  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putShort(requiredAcks)
    buffer.putInt(ackTimeoutMs)

    //save the topic structure
    buffer.putInt(dataGroupedByTopic.size) //the number of topics 缓存有多少个topic
    
    //循环每一个topic
    dataGroupedByTopic.foreach {
      //topicAndPartitionData表示Map[TopicAndPartition, ByteBufferMessageSet]
      case (topic, topicAndPartitionData) =>
        writeShortString(buffer, topic) //write the topic 写入topic信息
        buffer.putInt(topicAndPartitionData.size) //the number of partitions 写入该topic有多少个Map[TopicAndPartition, ByteBufferMessageSet]
        //循环该topic的每一个Map[TopicAndPartition, ByteBufferMessageSet]
        topicAndPartitionData.foreach(partitionAndData => {
          val partition = partitionAndData._1.partition//获取partition
          val partitionMessageData = partitionAndData._2//获取
          val bytes = partitionMessageData.buffer
          buffer.putInt(partition)
          buffer.putInt(bytes.limit)
          buffer.put(bytes)
          bytes.rewind
        })
    }
  }

  //该ProducerRequest占用的字节大小
  def sizeInBytes: Int = {
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) + /* client id */
    2 + /* requiredAcks */
    4 + /* ackTimeoutMs */
    4 + /* number of topics */
    dataGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      foldedTopics +
      shortStringLength(currTopic._1) +
      4 + /* the number of partitions */
      {
        currTopic._2.foldLeft(0)((foldedPartitions, currPartition) => {
          foldedPartitions +
          4 + /* partition id */
          4 + /* byte-length of serialized messages */
          currPartition._2.sizeInBytes
        })
      }
    })
  }

  //返回有多少个topic-partition组合数量
  def numPartitions = data.size

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    if(request.requestObj.asInstanceOf[ProducerRequest].requiredAcks == 0) {
        requestChannel.closeConnection(request.processor, request)
    }
    else {
      //data为Map[TopicAndPartition, ByteBufferMessageSet]
      val producerResponseStatus = data.map {
        case (topicAndPartition, data) =>
          (topicAndPartition, ProducerResponseStatus(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), -1l))
      }
      val errorResponse = ProducerResponse(correlationId, producerResponseStatus)
      requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
    }
  }

  //打印信息,相当于toString方法,参数true表示打印详细信息
  override def describe(details: Boolean): String = {
    val producerRequest = new StringBuilder
    producerRequest.append("Name: " + this.getClass.getSimpleName)
    producerRequest.append("; Version: " + versionId)
    producerRequest.append("; CorrelationId: " + correlationId)
    producerRequest.append("; ClientId: " + clientId)
    producerRequest.append("; RequiredAcks: " + requiredAcks)
    producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms")
    if(details)
      producerRequest.append("; TopicAndPartition: " + topicPartitionMessageSizeMap.mkString(","))
    producerRequest.toString()
  }

  //清空要发送的数据内容
  def emptyData(){
    data.clear()
  }
}

