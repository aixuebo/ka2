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

package kafka.consumer

import kafka.api._
import kafka.network._
import kafka.utils._
import kafka.common.{ErrorMapping, TopicAndPartition}
import org.apache.kafka.common.utils.Utils._

/**
 * A consumer of kafka messages
 * 一个broker的简单的消费者
 */
@threadsafe
class SimpleConsumer(val host: String,//broker的host和端口
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int,
                     val clientId: String) extends Logging {//准备去连接到该broker的消费者名字

  ConsumerConfig.validateClientId(clientId) //校验clientId格式是否符合标准
  private val lock = new Object()
  
  //连接该host:port所在的broker节点
  private val blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout)
  
  //用于统计该客户端抓取的信息
  private val fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId)
  private var isClosed = false

  //与该broker建立连接
  private def connect(): BlockingChannel = {
    close
    blockingChannel.connect()
    blockingChannel
  }

  //销毁与该broker的连接
  private def disconnect() = {
    debug("Disconnecting from " + formatAddress(host, port))
    blockingChannel.disconnect()
  }

  //重连
  private def reconnect() {
    disconnect()
    connect()
  }

  def close() {
    lock synchronized {
      disconnect()
      isClosed = true
    }
  }
  
  //同步发送数据,返回请求结果
  private def sendRequest(request: RequestOrResponse): Receive = {
    lock synchronized {
      var response: Receive = null
      try {
        getOrMakeConnection()
        blockingChannel.send(request)
        response = blockingChannel.receive()
      } catch {
        case e : Throwable =>
          info("Reconnect due to socket error: %s".format(e.toString))
          // retry once
          try {
            reconnect()
            blockingChannel.send(request)
            response = blockingChannel.receive()
          } catch {
            case e: Throwable =>
              disconnect()
              throw e
          }
      }
      response
    }
  }

  def send(request: TopicMetadataRequest): TopicMetadataResponse = {
    val response = sendRequest(request)
    TopicMetadataResponse.readFrom(response.buffer)
  }

  def send(request: ConsumerMetadataRequest): ConsumerMetadataResponse = {
    val response = sendRequest(request)
    ConsumerMetadataResponse.readFrom(response.buffer)
  }

  /**
   *  Fetch a set of messages from a topic.
   *
   *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
   *  @return a set of fetched messages
   *  抓取某些个topic的某些partition,从offset开始,抓取fetchSize个数据
   */
  def fetch(request: FetchRequest): FetchResponse = {
    var response: Receive = null
    val specificTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestTimer
    val aggregateTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestTimer
    aggregateTimer.time {
      specificTimer.time {
        response = sendRequest(request)
      }
    }
    val fetchResponse = FetchResponse.readFrom(response.buffer)
    val fetchedSize = fetchResponse.sizeInBytes
    fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestSizeHist.update(fetchedSize)
    fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestSizeHist.update(fetchedSize)
    fetchResponse
  }

  /**
   *  Get a list of valid offsets (up to maxSize) before the given time.
   *  @param request a [[kafka.api.OffsetRequest]] object.
   *  @return a [[kafka.api.OffsetResponse]] object.
   */
  def getOffsetsBefore(request: OffsetRequest) = OffsetResponse.readFrom(sendRequest(request).buffer)

  /**
   * Commit offsets for a topic
   * Version 0 of the request will commit offsets to Zookeeper and version 1 and above will commit offsets to Kafka.
   * @param request a [[kafka.api.OffsetCommitRequest]] object.
   * @return a [[kafka.api.OffsetCommitResponse]] object.
   */
  def commitOffsets(request: OffsetCommitRequest) = {
    // TODO: With KAFKA-1012, we have to first issue a ConsumerMetadataRequest and connect to the coordinator before
    // we can commit offsets.
    OffsetCommitResponse.readFrom(sendRequest(request).buffer)
  }

  /**
   * Fetch offsets for a topic
   * Version 0 of the request will fetch offsets from Zookeeper and version 1 version 1 and above will fetch offsets from Kafka.
   * @param request a [[kafka.api.OffsetFetchRequest]] object.
   * @return a [[kafka.api.OffsetFetchResponse]] object.
   */
  def fetchOffsets(request: OffsetFetchRequest) = OffsetFetchResponse.readFrom(sendRequest(request).buffer)

  private def getOrMakeConnection() {
    if(!isClosed && !blockingChannel.isConnected) {
      connect()
    }
  }

  /**
   * Get the earliest or latest offset of a given topic, partition.
   * @param topicAndPartition Topic and partition of which the offset is needed.要查找的topic-partition
   * @param earliestOrLatest A value to indicate earliest or latest offset.//要查找最早的还是最新的数据
   * @param consumerId Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.该id可以表示是消费者客户端、SimpleConsumerShell还是某一个follower从节点
   * @return Requested offset.返回最新的序号
   * 查找某一个topic-partition的某个时间点的序号
   */
  def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val request = OffsetRequest(requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1)),
                                clientId = clientId,
                                replicaId = consumerId)
    val partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition)
    val offset = partitionErrorAndOffset.error match {
      case ErrorMapping.NoError => partitionErrorAndOffset.offsets.head
      case _ => throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error)
    }
    offset
  }
}

