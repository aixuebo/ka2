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

import java.nio.ByteBuffer
import kafka.api.ApiUtils._
import kafka.utils.{SystemTime, Logging}
import kafka.network.{RequestChannel, BoundedByteBufferSend}
import kafka.common.{OffsetAndMetadata, ErrorMapping, TopicAndPartition}
import kafka.network.RequestChannel.Response
import scala.collection._

object OffsetCommitRequest extends Logging {
  val CurrentVersion: Short = 1
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): OffsetCommitRequest = {
    // Read values from the envelope
    val versionId = buffer.getShort
    assert(versionId == 0 || versionId == 1,
           "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0 or 1.")

    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)

    // Read the OffsetRequest 
    val consumerGroupId = readShortString(buffer)

    // version 1 specific fields
    var groupGenerationId: Int = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID
    var consumerId: String = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID
    if (versionId == 1) {
      groupGenerationId = buffer.getInt
      consumerId = readShortString(buffer)
    }

    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val timestamp = {
          if (versionId == 1) {
            val given = buffer.getLong
            given
          } else
            OffsetAndMetadata.InvalidTime
        }
        val metadata = readShortString(buffer)
        (TopicAndPartition(topic, partitionId), OffsetAndMetadata(offset, metadata, timestamp))
      })
    })
    OffsetCommitRequest(consumerGroupId, immutable.Map(pairs:_*), versionId, correlationId, clientId, groupGenerationId, consumerId)
  }

  //更改非法的时间戳为当前时间戳
  def changeInvalidTimeToCurrentTime(offsetCommitRequest: OffsetCommitRequest) {
    val now = SystemTime.milliseconds
    for ( (topicAndPartiiton, offsetAndMetadata) <- offsetCommitRequest.requestInfo)
      if (offsetAndMetadata.timestamp == OffsetAndMetadata.InvalidTime)
        offsetAndMetadata.timestamp = now
  }
}

//更新topic-partition的offset请求
case class OffsetCommitRequest(groupId: String,//因为offset是按照group进行partition分区的,因此这个参数很重要
                               requestInfo: immutable.Map[TopicAndPartition, OffsetAndMetadata],//哪些topic-partition对应的offset和偏移量的描述信息
                               versionId: Short = OffsetCommitRequest.CurrentVersion,//用于定义OffsetCommitRequest的信息,版本不同,可能是追加了新的字段导致的
                               correlationId: Int = 0,//@correlationId 请求的关联ID,用于回复给客户端后,客户端知道哪个失败
                               clientId: String = OffsetCommitRequest.DefaultClientId,
                               groupGenerationId: Int = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID,
                               consumerId: String =  org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID)
    extends RequestOrResponse(Some(RequestKeys.OffsetCommitKey)) {
  assert(versionId == 0 || versionId == 1,
         "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0 or 1.")
 //返回值Map[String, Map[TopicAndPartition, OffsetAndMetadata]],即按照topic分组
  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  //过滤掉描述信息>参数maxMetadataSize的数据,注意如果描述是null是会保留下来的
  def filterLargeMetadata(maxMetadataSize: Int) =
    requestInfo.filter(info => info._2.metadata == null || info._2.metadata.length <= maxMetadataSize)

    /**
     * @errorCode 异常的状态码
     * @offsetMetadataMaxSize 描述信息的最大长度
     */
  def responseFor(errorCode: Short, offsetMetadataMaxSize: Int) = {
    //返回Map[TopicAndPartition, Short] 每一个topic-partition作为key,value是状态码
    val commitStatus = requestInfo.map {info =>
      (info._1, if (info._2.metadata != null && info._2.metadata.length > offsetMetadataMaxSize)
                  ErrorMapping.OffsetMetadataTooLargeCode //offset的描述信息过长
                else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode) //没有topic-partition信息
                  ErrorMapping.ConsumerCoordinatorNotAvailableCode
                else if (errorCode == ErrorMapping.NotLeaderForPartitionCode) //partition没有leader
                  ErrorMapping.NotCoordinatorForConsumerCode
                else
                  errorCode)
    }.toMap
    OffsetCommitResponse(commitStatus, correlationId)
  }

  def writeTo(buffer: ByteBuffer) {
    // Write envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)

    // Write OffsetCommitRequest
    writeShortString(buffer, groupId)             // consumer group

    // version 1 specific data
    if (versionId == 1) {
      buffer.putInt(groupGenerationId)
      writeShortString(buffer, consumerId)
    }
    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics
    requestInfoGroupedByTopic.foreach( t1 => { // topic -> Map[TopicAndPartition, OffsetMetadataAndError]
      writeShortString(buffer, t1._1) // topic
      buffer.putInt(t1._2.size)       // number of partitions for this topic 该topic有多少个partition
      t1._2.foreach( t2 => {
        buffer.putInt(t2._1.partition) //记录每一个partition
        buffer.putLong(t2._2.offset) //记录该partition对应的offset
        if (versionId == 1)
          buffer.putLong(t2._2.timestamp) //记录时间戳
        writeShortString(buffer, t2._2.metadata) //记录描述信息
      })
    })
  }

  override def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    shortStringLength(groupId) +
    (if (versionId == 1) 4 /* group generation id */ + shortStringLength(consumerId) else 0) +
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((count, topicAndOffsets) => {
      val (topic, offsets) = topicAndOffsets
      count +
      shortStringLength(topic) + /* topic */
      4 + /* number of partitions */
      offsets.foldLeft(0)((innerCount, offsetAndMetadata) => {
        innerCount +
        4 /* partition */ +
        8 /* offset */ +
        (if (versionId == 1) 8 else 0 ) /* timestamp */ +
        shortStringLength(offsetAndMetadata._2.metadata)
      })
    })

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]) //异常的映射码
    val errorResponse = responseFor(errorCode, Int.MaxValue)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse))) //发送response
  }

  override def describe(details: Boolean): String = {
    val offsetCommitRequest = new StringBuilder
    offsetCommitRequest.append("Name: " + this.getClass.getSimpleName)
    offsetCommitRequest.append("; Version: " + versionId)
    offsetCommitRequest.append("; CorrelationId: " + correlationId)
    offsetCommitRequest.append("; ClientId: " + clientId)
    offsetCommitRequest.append("; GroupId: " + groupId)
    offsetCommitRequest.append("; GroupGenerationId: " + groupGenerationId)
    offsetCommitRequest.append("; ConsumerId: " + consumerId)
    if(details)
      offsetCommitRequest.append("; RequestInfo: " + requestInfo.mkString(","))
    offsetCommitRequest.toString()
  }

  override def toString = {
    describe(details = true)
  }
}
