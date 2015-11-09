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

package kafka.producer

import kafka.api._
import kafka.network.{BlockingChannel, BoundedByteBufferSend, Receive}
import kafka.utils._
import java.util.Random

import org.apache.kafka.common.utils.Utils._

object SyncProducer {
  val RequestKey: Short = 0
  val randomGenerator = new Random
}

/*
 * Send a message set.
 * 同步生产者客户端
 */
@threadsafe
class SyncProducer(val config: SyncProducerConfig) extends Logging {

  private val lock = new Object()
  @volatile private var shutdown: Boolean = false
  
  //建立与该host、port服务器连接的请求
  private val blockingChannel = new BlockingChannel(config.host, config.port, BlockingChannel.UseDefaultBufferSize,
    config.sendBufferBytes, config.requestTimeoutMs)
  
  //注册该生产者的请求统计信息状态
  val producerRequestStats = ProducerRequestStatsRegistry.getProducerRequestStats(config.clientId)

  //打印日志,同步生产者的初始化的配置信息
  trace("Instantiating Scala Sync Producer with properties: %s".format(config.props))

  //校验请求,该请求校验仅仅向log4j日志中打印trace日志,即仅仅与log4j有关系的校验
  private def verifyRequest(request: RequestOrResponse) = {
    /**
     * 这个校验看起来有一些费解,但是这个想法来自校验简单的更改log4j设置,
     * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
     * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
     * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
     */
    if (logger.isDebugEnabled) {
      val buffer = new BoundedByteBufferSend(request).buffer//获取请求的buffer信息
      trace("verifying sendbuffer of size " + buffer.limit)
      val requestTypeId = buffer.getShort()//获取请求的类型
      if(requestTypeId == RequestKeys.ProduceKey) {//该请求类型必须是生产者请求类型,
        val request = ProducerRequest.readFrom(buffer)//反序列化该生产者请求对象
        trace(request.toString)
      }
    }
  }

  /**
   * Common functionality for the public send methods
   * 该方法是真正的向服务器发送request请求
   * 
   * 参数readResponse,true表示要读取服务器的返回值,false表示不读取服务器的返回值,默认值是true,需要服务器返回内容
   * return 返回服务器的返回值输入流
   */
  private def doSend(request: RequestOrResponse, readResponse: Boolean = true): Receive = {
    lock synchronized {
      
      //校验请求,并且与服务器建立连接,注意:连接仅仅会建立依次
      verifyRequest(request)
      getOrMakeConnection()

      var response: Receive = null
      try {
        blockingChannel.send(request)//发送该请求
        if(readResponse)
          response = blockingChannel.receive()//读取服务器的返回值
        else
          trace("Skipping reading response")
      } catch {
        case e: java.io.IOException =>
          // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
          disconnect()
          throw e
        case e: Throwable => throw e
      }
      response
    }
  }

  /**
   * Send a message. If the producerRequest had required.request.acks=0, then the
   * returned response object is null
   * 向服务器发送生产者请求,返回生产者对象
   */
  def send(producerRequest: ProducerRequest): ProducerResponse = {
    val requestSize = producerRequest.sizeInBytes//请求的字节大小,用于计算统计信息
    producerRequestStats.getProducerRequestStats(config.host, config.port).requestSizeHist.update(requestSize)
    producerRequestStats.getProducerRequestAllBrokersStats.requestSizeHist.update(requestSize)

    var response: Receive = null
    
    //以下两个对象用于统计请求时间
    val specificTimer = producerRequestStats.getProducerRequestStats(config.host, config.port).requestTimer
    val aggregateTimer = producerRequestStats.getProducerRequestAllBrokersStats.requestTimer
    aggregateTimer.time {
      specificTimer.time {
        //发送请求,并且设置第二个参数是否需要返回值
        response = doSend(producerRequest, if(producerRequest.requiredAcks == 0) false else true)
      }
    }
    if(producerRequest.requiredAcks != 0)//不等于0,说明需要确定数据返回值,因此要解析response,转化成ProducerResponse对象
      ProducerResponse.readFrom(response.buffer)
    else
      null
  }

  /**
   * 发送获取topic元数据的请求
   * return 将服务器返回的结果反序列化成TopicMetadataResponse对象返回
   */
  def send(request: TopicMetadataRequest): TopicMetadataResponse = {
    val response = doSend(request)//向服务器发送请求,并且等待服务器返回结果
    TopicMetadataResponse.readFrom(response.buffer)//将服务器返回的结果反序列化成TopicMetadataResponse对象返回
  }

  def close() = {
    lock synchronized {
      disconnect()
      shutdown = true
    }
  }

  /**
   * Disconnect from current channel, closing connection.
   * Side effect: channel field is set to null on successful disconnect
   * 断开与服务器连接
   */
  private def disconnect() {
    try {
      info("Disconnecting from " + formatAddress(config.host, config.port))
      blockingChannel.disconnect()
    } catch {
      case e: Exception => error("Error on disconnect: ", e)
    }
  }

  //与服务器创建连接
  private def connect(): BlockingChannel = {
    if (!blockingChannel.isConnected && !shutdown) {
      try {
        blockingChannel.connect()
        info("Connected to " + formatAddress(config.host, config.port) + " for producing")
      } catch {
        case e: Exception => {
          disconnect()
          error("Producer connection to " + formatAddress(config.host, config.port) + " unsuccessful", e)
          throw e
        }
      }
    }
    blockingChannel
  }

  //如果服务器尚未连接上,则连接服务器
  private def getOrMakeConnection() {
    if(!blockingChannel.isConnected) {
      connect()
    }
  }
}