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

import kafka.utils.Logging
import kafka.common.{ErrorMapping, TopicAndPartition}

object OffsetCommitResponse extends Logging {
  val CurrentVersion: Short = 1

  def readFrom(buffer: ByteBuffer): OffsetCommitResponse = {
    val correlationId = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = ApiUtils.readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val error = buffer.getShort
        (TopicAndPartition(topic, partitionId), error)
      })
    })
    OffsetCommitResponse(Map(pairs:_*), correlationId)
  }
}

/**
 *  Single constructor for both version 0 and 1 since they have the same format.
 *  @commitStatus Map[TopicAndPartition, Short] 每一个topic-partition作为key,value是状态码
 *  @correlationId 请求的关联ID,用于回复给客户端后,客户端知道哪个失败
 */
case class OffsetCommitResponse(commitStatus: Map[TopicAndPartition, Short],
                                correlationId: Int = 0)
    extends RequestOrResponse() {

  //返回值Map[String, Map[TopicAndPartition, Short]],即按照topic进行了分组,key是topic
  lazy val commitStatusGroupedByTopic = commitStatus.groupBy(_._1.topic)

  //是否存在异常的数据,true表示存在异常数据
  def hasError = commitStatus.exists{ case (topicAndPartition, errorCode) => errorCode != ErrorMapping.NoError }

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(correlationId)
    buffer.putInt(commitStatusGroupedByTopic.size)
    commitStatusGroupedByTopic.foreach { case(topic, statusMap) =>
      ApiUtils.writeShortString(buffer, topic) //写入topic
      buffer.putInt(statusMap.size) // partition count 写入该topic的partition数
      statusMap.foreach { case(topicAndPartition, errorCode) =>
        buffer.putInt(topicAndPartition.partition) //写入每一个partition编号
        buffer.putShort(errorCode) //写入该partition对应的状态码
      }
    }
  }

  override def sizeInBytes = 
    4 + /* correlationId */
    4 + /* topic count */
    commitStatusGroupedByTopic.foldLeft(0)((count, partitionStatusMap) => {
      val (topic, partitionStatus) = partitionStatusMap
      count +
      ApiUtils.shortStringLength(topic) +
      4 + /* partition count */
      partitionStatus.size * ( 4 /* partition */  + 2 /* error code */)
    })

  override def describe(details: Boolean):String = { toString }

}

