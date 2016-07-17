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

import kafka.utils.nonthreadsafe
import kafka.api.ApiUtils._
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.network.RequestChannel
import kafka.message.MessageSet

import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer
import scala.collection.immutable.Map

//表示抓取该Partition的数据最多抓取fetchSize个字节数据,从offset开始抓取
case class PartitionFetchInfo(offset: Long, fetchSize: Int)

//抓取某些个topic的某些partition,从offset开始,抓取fetchSize个数据
object FetchRequest {
  val CurrentVersion = 0.shortValue
  val DefaultMaxWait = 0
  val DefaultMinBytes = 0
  val DefaultCorrelationId = 0

  def readFrom(buffer: ByteBuffer): FetchRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val replicaId = buffer.getInt
    val maxWait = buffer.getInt
    val minBytes = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val fetchSize = buffer.getInt
        (TopicAndPartition(topic, partitionId), PartitionFetchInfo(offset, fetchSize))
      })
    })
    FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, Map(pairs:_*))
  }
}

/**
 * 
 * 参数requestInfo Map[TopicAndPartition, PartitionFetchInfo],key表示抓取哪个topic-partition数据,value表示从offset开始抓取,抓取多少个数据返回
 */
case class FetchRequest(versionId: Short = FetchRequest.CurrentVersion,//版本号,固定值
                        correlationId: Int = FetchRequest.DefaultCorrelationId,//自动会累加1,表示发送请求的序号
                        clientId: String = ConsumerConfig.DefaultClientId,//客户端的唯一名称,区分是哪个客户端发过来的数据
                        replicaId: Int = Request.OrdinaryConsumerId,//哪个follower节点发过来的抓去请求
                        maxWait: Int = FetchRequest.DefaultMaxWait,//发送回复信息的最大等候时间,超过该时间,则立刻发送出去
                        minBytes: Int = FetchRequest.DefaultMinBytes,//最小发送回复信息的数据量
                        requestInfo: Map[TopicAndPartition, PartitionFetchInfo])//发送本次抓取请求,是抓取那些topic-partition,从第几个序号开始抓取,最多抓取多少个字节
        extends RequestOrResponse(Some(RequestKeys.FetchKey)) {

  /**
   * Partitions the request info into a map of maps (one for each topic).
   * 进行按照topic分组
   * Map[String, Map[TopicAndPartition, PartitionFetchInfo]] key是topic,value中key表示抓取哪个topic-partition数据,value表示从offset开始抓取,抓取多少个数据返回
   */
  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  /**
   *  Public constructor for the clients
   */
  def this(correlationId: Int,
           clientId: String,
           maxWait: Int,
           minBytes: Int,
           requestInfo: Map[TopicAndPartition, PartitionFetchInfo]) {
    this(versionId = FetchRequest.CurrentVersion,
         correlationId = correlationId,
         clientId = clientId,
         replicaId = Request.OrdinaryConsumerId,//默认就是-1,表示就是纯粹的消费者客户端,不是follow节点
         maxWait = maxWait,
         minBytes= minBytes,
         requestInfo = requestInfo)
  }

  //将请求的信息写入到buffer中
  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(replicaId)
    buffer.putInt(maxWait)
    buffer.putInt(minBytes)
    buffer.putInt(requestInfoGroupedByTopic.size) // topic count 要抓取多少个topic数据
    //写入抓取某个topic的某个partition,从offset开始,抓取fetchSize个数据

    /**
     * 循环Map[String, Map[TopicAndPartition, PartitionFetchInfo]]
     * key就是topic
     * value就是该topic下的一个集合,其中key是同一个topic下要抓取哪些partition,value是该partition对应从哪个序号开始抓取,抓取最多多少个字节
     */
    requestInfoGroupedByTopic.foreach {
      case (topic, partitionFetchInfos) =>
        writeShortString(buffer, topic)//输出topic名称
        buffer.putInt(partitionFetchInfos.size) // partition count 输出抓取多少个该topic的partition
        partitionFetchInfos.foreach {//循环该topic下每一个partition
          case (TopicAndPartition(_, partition), PartitionFetchInfo(offset, fetchSize)) =>
            buffer.putInt(partition)//写入要抓取哪个partition
            buffer.putLong(offset)//从哪个序号开始抓取
            buffer.putInt(fetchSize)//最多抓取多少字节数据
        }
    }
  }

  def sizeInBytes: Int = {
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    4 + /* replicaId */
    4 + /* maxWait */
    4 + /* minBytes */
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      val (topic, partitionFetchInfos) = currTopic
      foldedTopics +
      shortStringLength(topic) +
      4 + /* partition count */
      partitionFetchInfos.size * (
        4 + /* partition id */
        8 + /* offset */
        4 /* fetch size */
      )
    })
  }

  def isFromFollower = Request.isValidBrokerId(replicaId)//true表示该请求来自于follower节点

  def isFromOrdinaryConsumer = replicaId == Request.OrdinaryConsumerId//从纯粹的普通的消费者客户端去抓取的请求,不是follow节点

  def isFromLowLevelConsumer = replicaId == Request.DebuggingConsumerId//debug级别的,更低级别的客户端请求的抓取

  def numPartitions = requestInfo.size//抓取多少个partition

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val fetchResponsePartitionData = requestInfo.map {
      case (topicAndPartition, data) =>
        (topicAndPartition, FetchResponsePartitionData(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), -1, MessageSet.Empty))
    }
    val errorResponse = FetchResponse(correlationId, fetchResponsePartitionData)
    requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val fetchRequest = new StringBuilder
    fetchRequest.append("Name: " + this.getClass.getSimpleName)
    fetchRequest.append("; Version: " + versionId)
    fetchRequest.append("; CorrelationId: " + correlationId)
    fetchRequest.append("; ClientId: " + clientId)
    fetchRequest.append("; ReplicaId: " + replicaId)
    fetchRequest.append("; MaxWait: " + maxWait + " ms")
    fetchRequest.append("; MinBytes: " + minBytes + " bytes")
    if(details)
      fetchRequest.append("; RequestInfo: " + requestInfo.mkString(","))
    fetchRequest.toString()
  }
}

//用于配置创建抓取请求对象
@nonthreadsafe
class FetchRequestBuilder() {
  private val correlationId = new AtomicInteger(0)//每次请求的时候会自动累加1
  private val versionId = FetchRequest.CurrentVersion
  private var clientId = ConsumerConfig.DefaultClientId//发送请求的客户端标示
  private var replicaId = Request.OrdinaryConsumerId//消费者节点,如果是纯粹的客户端,则该值是-1,如果是集群上的follow节点,则该值有意义
  private var maxWait = FetchRequest.DefaultMaxWait//如果没有充足的数据去立即满足抓取的最小值,则在返回给抓取请求客户端之前,在server对岸最大的停留时间
  private var minBytes = FetchRequest.DefaultMinBytes//一次抓取请求,server最小返回给客户端的字节数,如果请求不足这些数据,则请求将会被阻塞
  
  //key是准备抓取partition,value是从哪个位置开始抓取,准备抓取多少个字节
  private val requestMap = new collection.mutable.HashMap[TopicAndPartition, PartitionFetchInfo]

  //添加要抓取topic-partition上从offset开始,最多fetchSize个字节数据
  def addFetch(topic: String, partition: Int, offset: Long, fetchSize: Int) = {
    requestMap.put(TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize))
    this
  }

  def clientId(clientId: String): FetchRequestBuilder = {
    this.clientId = clientId
    this
  }

  /**
   * Only for internal use. Clients shouldn't set replicaId.
   */
  private[kafka] def replicaId(replicaId: Int): FetchRequestBuilder = {
    this.replicaId = replicaId
    this
  }

  def maxWait(maxWait: Int): FetchRequestBuilder = {
    this.maxWait = maxWait
    this
  }

  def minBytes(minBytes: Int): FetchRequestBuilder = {
    this.minBytes = minBytes
    this
  }

  //创建一个抓取对象请求
  def build() = {
    //生成要抓取的请求信息
    val fetchRequest = FetchRequest(versionId, correlationId.getAndIncrement, clientId, replicaId, maxWait, minBytes, requestMap.toMap)
    requestMap.clear()//情况要抓取的信息内容
    fetchRequest
  }
}
