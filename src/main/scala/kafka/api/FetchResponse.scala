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
import java.nio.channels.GatheringByteChannel

import kafka.common.{TopicAndPartition, ErrorMapping}
import kafka.message.{MessageSet, ByteBufferMessageSet}
import kafka.network.{MultiSend, Send}
import kafka.api.ApiUtils._

//参见RequestKeys.FetchKey
//抓取某些个topic的某些partition,从offset开始,抓取fetchSize个数据 的返回对象
object FetchResponsePartitionData {
  def readFrom(buffer: ByteBuffer): FetchResponsePartitionData = {
    val error = buffer.getShort
    val hw = buffer.getLong
    val messageSetSize = buffer.getInt//返回的字节长度
    val messageSetBuffer = buffer.slice()//复制一份
    messageSetBuffer.limit(messageSetSize)//设置复制后的有效字节最后一个位置
    buffer.position(buffer.position + messageSetSize)//因为已经有效字节已经存在了一个复制版本,因此buffer的position进行向前滚动,抛出这个回复的数据信息
    new FetchResponsePartitionData(error, hw, new ByteBufferMessageSet(messageSetBuffer))
  }

  val headerSize =
    2 + /* error code */
    8 + /* high watermark */
    4 /* messageSetSize 字节大小所占用的字节 */
}

/**
 * hw 表示leader的partition数据已经写入到log文件的哪个字节位置
 */
case class FetchResponsePartitionData(error: Short = ErrorMapping.NoError, hw: Long = -1L, messages: MessageSet) {
  val sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes //总字节数
}

// SENDS
//发送一个partition的数据
class PartitionDataSend(val partitionId: Int,//partitionId
                        val partitionData: FetchResponsePartitionData) extends Send {//partition对应的数据内容
  private val messageSize = partitionData.messages.sizeInBytes
  private var messagesSentSize = 0

  private val buffer = ByteBuffer.allocate( 4 /** partitionId **/ + FetchResponsePartitionData.headerSize)
  buffer.putInt(partitionId)
  buffer.putShort(partitionData.error)
  buffer.putLong(partitionData.hw)
  buffer.putInt(partitionData.messages.sizeInBytes)
  buffer.rewind()

  override def complete = !buffer.hasRemaining && messagesSentSize >= messageSize

  //将信息写出到GatheringByteChannel中
  override def writeTo(channel: GatheringByteChannel): Int = {
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && messagesSentSize < messageSize) {
      val bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize)
      messagesSentSize += bytesSent
      written += bytesSent
    }
    written
  }
}

//每一个topic对应一组partition--partition对应的数据
object TopicData {
  def readFrom(buffer: ByteBuffer): TopicData = {
    val topic = readShortString(buffer)//topic内容
    val partitionCount = buffer.getInt//该tipic多少个partition
    val topicPartitionDataPairs = (1 to partitionCount).map(_ => {
      val partitionId = buffer.getInt//partition的ID
      val partitionData = FetchResponsePartitionData.readFrom(buffer)//该partition的内容
      (partitionId, partitionData)
    })
    TopicData(topic, Map(topicPartitionDataPairs:_*))//每一个topic对应一组partition--partition对应的数据
  }

  def headerSize(topic: String) =
    shortStringLength(topic) +
    4 /* partition count */
}

//每一个topic对应一组partition--partition对应的数据
case class TopicData(topic: String, partitionData: Map[Int, FetchResponsePartitionData]) {//key是partitionID,value是该partition的内容
  val sizeInBytes =
    TopicData.headerSize(topic) + partitionData.values.foldLeft(0)(_ + _.sizeInBytes + 4)

  val headerSize = TopicData.headerSize(topic)
}

//发送整个topic数据
class TopicDataSend(val topicData: TopicData) extends Send {
  private val size = topicData.sizeInBytes

  private var sent = 0

  override def complete = sent >= size

  private val buffer = ByteBuffer.allocate(topicData.headerSize)
  writeShortString(buffer, topicData.topic)//写入topic名称
  buffer.putInt(topicData.partitionData.size)//写入该topic的所有partition数据
  buffer.rewind()

  val sends = new MultiSend(topicData.partitionData.toList
                                    .map(d => new PartitionDataSend(d._1, d._2))) {
    val expectedBytesToWrite = topicData.sizeInBytes - topicData.headerSize
  }

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeTo(channel)
    }
    sent += written
    written
  }
}


object FetchResponse {

  val headerSize =
    4 + /* correlationId */
    4 /* topic count */

  def readFrom(buffer: ByteBuffer): FetchResponse = {
    val correlationId = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topicData = TopicData.readFrom(buffer)
      topicData.partitionData.map {
        case (partitionId, partitionData) =>
          (TopicAndPartition(topicData.topic, partitionId), partitionData)
      }
    })
    FetchResponse(correlationId, Map(pairs:_*))
  }
}


/**
 *
 * @param correlationId 客户端哪个序号对应的请求被返回了,每一个请求有一个自增长的ID
 * @param data 对应返回的每一个topic-partition对应的数据抓取回来的数据映射关系
 */
case class FetchResponse(correlationId: Int,
                         data: Map[TopicAndPartition, FetchResponsePartitionData])
    extends RequestOrResponse() {

  /**
   * Partitions the data into a map of maps (one for each topic).
   * 返回结果转换成Map[String,Map[TopicAndPartition, FetchResponsePartitionData]],即按照topic进行分组
   * 转换的目的就是多个topic-partition可以只传输一个topic就可以了,减少传输数据量
   */
  lazy val dataGroupedByTopic = data.groupBy(_._1.topic)

  //头所占字节+返回的数据所占字节
  val sizeInBytes =
    FetchResponse.headerSize +
    dataGroupedByTopic.foldLeft(0) ((folded, curr) => {
      val topicData = TopicData(curr._1, curr._2.map {
        case (topicAndPartition, partitionData) => (topicAndPartition.partition, partitionData)
      })
      folded + topicData.sizeInBytes
    })

  /*
   * FetchResponse uses [sendfile](http://man7.org/linux/man-pages/man2/sendfile.2.html)
   * api for data transfer, so `writeTo` aren't actually being used.
   * It is implemented as an empty function to comform to `RequestOrResponse.writeTo`
   * abstract method signature.
   */
  def writeTo(buffer: ByteBuffer): Unit = throw new UnsupportedOperationException

  override def describe(details: Boolean): String = toString

  private def partitionDataFor(topic: String, partition: Int): FetchResponsePartitionData = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    data.get(topicAndPartition) match {
      case Some(partitionData) => partitionData
      case _ =>
        throw new IllegalArgumentException(
          "No partition %s in fetch response %s".format(topicAndPartition, this.toString))
    }
  }

  //获取某个topic-partition对应的messageBuffer数据
  def messageSet(topic: String, partition: Int): ByteBufferMessageSet =
    partitionDataFor(topic, partition).messages.asInstanceOf[ByteBufferMessageSet]

  //获取某个topic-partition对应的hw数据
  def highWatermark(topic: String, partition: Int) = partitionDataFor(topic, partition).hw

  //true表示有异常
  def hasError = data.values.exists(_.error != ErrorMapping.NoError)

  //获取某个topic-partition对应的状态码数据
  def errorCode(topic: String, partition: Int) = partitionDataFor(topic, partition).error
}


class FetchResponseSend(val fetchResponse: FetchResponse) extends Send {
  private val size = fetchResponse.sizeInBytes

  private var sent = 0//已经发送的字节数

  private val sendSize = 4 /* for size */ + size //最终要发送的字节数,4表示的是回复的总字节长度

  override def complete = sent >= sendSize //true表示发送完成

  private val buffer = ByteBuffer.allocate(4 /* for size */ + FetchResponse.headerSize)
  buffer.putInt(size)//回复的总字节长度
  buffer.putInt(fetchResponse.correlationId)//客户端请求ID
  buffer.putInt(fetchResponse.dataGroupedByTopic.size) // topic count topic数量
  buffer.rewind()

  /**
    //1.dataGroupedByTopic.toList 表示对map进行转换成list,好方便调用list的map方法,对每一个元素进行处理
    //转换成list的目的是MultiSend方法参数就是list类型的,因此list的map方法的返回值还是list
    //2.因此list的每一个元素就是String,Map[TopicAndPartition, FetchResponsePartitionData]
    //3.对每一个元素进行map处理
    //4.因此case(topic, data) 就代表字符串的topic和data是Map[TopicAndPartition, FetchResponsePartitionData]
    //5.因此可以组装成TopicDataSend对象
   */
  val sends = new MultiSend(fetchResponse.dataGroupedByTopic.toList.map {
    case(topic, data) => new TopicDataSend(TopicData(topic,
                                                     data.map{case(topicAndPartition, message) => (topicAndPartition.partition, message)}))
  }) {
    val expectedBytesToWrite = fetchResponse.sizeInBytes - FetchResponse.headerSize//期待写入的字节数
  }

  def writeTo(channel: GatheringByteChannel):Int = {
    expectIncomplete()//期望是不完成,如果完成则抛异常
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)//将buffer的内容写出到channel中.并且累加写了多少个字节,现在的buffer是头信息
    if(!buffer.hasRemaining && !sends.complete) {//说明buffer没有数据了,并且还没有发送完
      written += sends.writeTo(channel)//将sends的信息写入到channel中,并且累加输出字节数
    }
    sent += written//设置总共发送了多少个字节
    written//返回本次发送了多少个字节
  }
}

