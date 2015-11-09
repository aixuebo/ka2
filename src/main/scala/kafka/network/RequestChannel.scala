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

package kafka.network

import java.util.concurrent._
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import java.nio.ByteBuffer
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.utils.{Logging, SystemTime}
import kafka.message.ByteBufferMessageSet
import java.net._
import org.apache.log4j.Logger

/**
 * request的请求队列,全局性质的,属于server端的对象
 */
object RequestChannel extends Logging {
  val AllDone = new Request(1, 2, getShutdownReceive(), 0)

  //返回一个空的生产者对象
  def getShutdownReceive() = {
    val emptyProducerRequest = new ProducerRequest(0, 0, "", 0, 0, collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]())
    val byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes + 2)
    byteBuffer.putShort(RequestKeys.ProduceKey)
    emptyProducerRequest.writeTo(byteBuffer)
    byteBuffer.rewind()
    byteBuffer
  }

  /**
   * buffer参数,包含了RequestKeys中类型以及该类型对应的所有字节信息
   */
  case class Request(processor: Int, requestKey: Any, private var buffer: ByteBuffer, startTimeMs: Long, remoteAddress: SocketAddress = new InetSocketAddress(0)) {
    @volatile var requestDequeueTimeMs = -1L//请求出队列时间
    @volatile var apiLocalCompleteTimeMs = -1L//KafKaApi最终完成事件
    @volatile var responseCompleteTimeMs = -1L
    @volatile var responseDequeueTimeMs = -1L
    
    //通过buffer还原类型信息和RequestOrResponse类型对象
    val requestId = buffer.getShort()
    val requestObj: RequestOrResponse = RequestKeys.deserializerForKey(requestId)(buffer)
    buffer = null
    private val requestLogger = Logger.getLogger("kafka.request.logger")
    //打印哪个处理器要处理该RequestOrResponse请求
    trace("Processor %d received request : %s".format(processor, requestObj))

    def updateRequestMetrics() {
      val endTimeMs = SystemTime.milliseconds
      // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes since the remote
      // processing time is really small. In this case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
      if (apiLocalCompleteTimeMs < 0)
        apiLocalCompleteTimeMs = responseCompleteTimeMs
      val requestQueueTime = (requestDequeueTimeMs - startTimeMs).max(0L)
      val apiLocalTime = (apiLocalCompleteTimeMs - requestDequeueTimeMs).max(0L)
      val apiRemoteTime = (responseCompleteTimeMs - apiLocalCompleteTimeMs).max(0L)
      val responseQueueTime = (responseDequeueTimeMs - responseCompleteTimeMs).max(0L)
      val responseSendTime = (endTimeMs - responseDequeueTimeMs).max(0L)
      val totalTime = endTimeMs - startTimeMs
      
      //获取该分类要统计的维度集合
      var metricsList = List(RequestMetrics.metricsMap(RequestKeys.nameForKey(requestId)))
      
      if (requestId == RequestKeys.FetchKey) {//追加统计维度
        val isFromFollower = requestObj.asInstanceOf[FetchRequest].isFromFollower
        metricsList ::= ( if (isFromFollower)
                            RequestMetrics.metricsMap(RequestMetrics.followFetchMetricName)
                          else
                            RequestMetrics.metricsMap(RequestMetrics.consumerFetchMetricName) )
      }
      
      //对每一个统计信息进行更新
      metricsList.foreach{
        m => m.requestRate.mark()
             m.requestQueueTimeHist.update(requestQueueTime)
             m.localTimeHist.update(apiLocalTime)
             m.remoteTimeHist.update(apiRemoteTime)
             m.responseQueueTimeHist.update(responseQueueTime)
             m.responseSendTimeHist.update(responseSendTime)
             m.totalTimeHist.update(totalTime)
      }
      
      //if else 唯一的区别就是打印request的日志详细程度
      if(requestLogger.isTraceEnabled)
        requestLogger.trace("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d"
          .format(requestObj.describe(true), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime))
      else if(requestLogger.isDebugEnabled) {
        requestLogger.debug("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d"
          .format(requestObj.describe(false), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime))
      }
    }
  }
  
  /**
   * @param processor 表示该Request属于哪个处理器的Request
   * @param Request 表示该回复是回复哪个Request的请求
   * @param responseSend 表示该response要发送给request哪些数据
   * @param responseAction 表示该response是否要发送给request数据
   */
  case class Response(processor: Int, request: Request, responseSend: Send, responseAction: ResponseAction) {
    request.responseCompleteTimeMs = SystemTime.milliseconds

    def this(processor: Int, request: Request, responseSend: Send) =
      this(processor, request, responseSend, if (responseSend == null) NoOpAction else SendAction)

    def this(request: Request, send: Send) =
      this(request.processor, request, send)
  }

  trait ResponseAction
  case object SendAction extends ResponseAction//表示需要发送response给request
  case object NoOpAction extends ResponseAction//表示不需要发送response给request
  case object CloseConnectionAction extends ResponseAction//表示连接已经关闭,没办法发送response给request
}

//表示请求的队列
class RequestChannel(val numProcessors: Int, val queueSize: Int) extends KafkaMetricsGroup {
  private var responseListeners: List[(Int) => Unit] = Nil//该List集合存储int值
  
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)//该阻塞队列存储RequestChannel.Request对象,
  //每一个处理器拥有一个队列,每一个队列存储Response对象
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
  for(i <- 0 until numProcessors)
    responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()

  newGauge(
    "RequestQueueSize",
    new Gauge[Int] {
      def value = requestQueue.size
    }
  )

  newGauge("ResponseQueueSize", new Gauge[Int]{
    def value = responseQueues.foldLeft(0) {(total, q) => total + q.size()}
  })

  for (i <- 0 until numProcessors) {
    newGauge("ResponseQueueSize",
      new Gauge[Int] {
        def value = responseQueues(i).size()
      },
      Map("processor" -> i.toString)
    )
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }
  
  /** Send a response back to the socket server to be sent over the network */ 
  def sendResponse(response: RequestChannel.Response) {
    responseQueues(response.processor).put(response)
    for(onResponse <- responseListeners)
      onResponse(response.processor)
  }

  /** No operation to take for the request, need to read more over the network 添加一个不需要response的request请求 */
  def noOperation(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.NoOpAction))
    for(onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Close the connection for the request 添加一个与request请求断开的请求*/
  def closeConnection(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction))
    for(onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Get the next request or block until specified time has elapsed 
   *  获取一个request请求,如果没有请求,则等待 timeout时间,如果还没有request,则返回null
   **/
  def receiveRequest(timeout: Long): RequestChannel.Request =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one 
   *  阻塞方式获取下一个request对象,没有则一直阻塞获取
   **/
  def receiveRequest(): RequestChannel.Request =
    requestQueue.take()

  /** Get a response for the given processor if there is one 
   *  从指定处理器队列中获取一个response对象,非阻塞的,如果队列没有response,则返回null 
   **/
  def receiveResponse(processor: Int): RequestChannel.Response = {
    val response = responseQueues(processor).poll()//非阻塞的,如果队列没有response,则返回null 
    if (response != null)//如果获取到了response,则设置该response对应的request的responseDequeueTimeMs为当前时间
      response.request.responseDequeueTimeMs = SystemTime.milliseconds
    response
  }

  //添加监听
  def addResponseListener(onResponse: Int => Unit) { 
    responseListeners ::= onResponse
  }

  //清空request请求队列
  def shutdown() {
    requestQueue.clear
  }
}

//统计信息
object RequestMetrics {
  //每一个RequestKeys的name对应一个统计对象RequestMetrics
  val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
  val consumerFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "Consumer"
  val followFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "Follower"
  (RequestKeys.keyToNameAndDeserializerMap.values.map(e => e._1)
    ++ List(consumerFetchMetricName, followFetchMetricName)).foreach(name => metricsMap.put(name, new RequestMetrics(name)))
}

//统计的维度,每一个RequestKeys的name对应一个该统计信息
class RequestMetrics(name: String) extends KafkaMetricsGroup {
  val tags = Map("request" -> name)//创建一个Map,里面包含一个元组,key是request字符串,value是name
  val requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)
  // time a request spent in a request queue 该请求在请求队列中消耗的时间
  val requestQueueTimeHist = newHistogram("RequestQueueTimeMs", biased = true, tags)
  // time a request takes to be processed at the local broker在本地节点上该请求被消耗的时间
  val localTimeHist = newHistogram("LocalTimeMs", biased = true, tags)
  // time a request takes to wait on remote brokers (only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram("RemoteTimeMs", biased = true, tags)
  // time a response spent in a response queue ,response回复在resonse队列中消耗的时间
  val responseQueueTimeHist = newHistogram("ResponseQueueTimeMs", biased = true, tags)
  // time to send the response to the requester,response回复消耗的时间
  val responseSendTimeHist = newHistogram("ResponseSendTimeMs", biased = true, tags)
  //总时间
  val totalTimeHist = newHistogram("TotalTimeMs", biased = true, tags)
}

