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

package kafka.server

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.TimeUnit
import com.yammer.metrics.core.Meter

/**
 * A thread that answers kafka requests.
 * 处理socket的请求,从该socketserver中不断拿到request请求去处理
 */
class KafkaRequestHandler(id: Int,//第几个线程
                          brokerId: Int,//节点所在ID
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: Int,//总线程数量
                          val requestChannel: RequestChannel,//从全局的request队列中获取请求
                          apis: KafkaApis) extends Runnable with Logging {//kafka的api,可以知道请求是什么含义
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "

  def run() {
    while(true) {
      try {
        var req : RequestChannel.Request = null
        while (req == null) {//只要没取到request,就不断获取
          // We use a single meter for aggregate idle percentage for the thread pool.
          // Since meter is calculated as total_recorded_value / time_window and
          // time_window is independent of the number of threads, each recorded idle
          // time should be discounted by # threads.
          val startSelectTime = SystemTime.nanoseconds
          req = requestChannel.receiveRequest(300)//获取一个request
          val idleTime = SystemTime.nanoseconds - startSelectTime
          aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
        }

        if(req eq RequestChannel.AllDone) {//说明是关闭程序
          debug("Kafka request handler %d on broker %d received shut down command".format(
            id, brokerId))
          return
        }
        //请求出队列时间
        req.requestDequeueTimeMs = SystemTime.milliseconds
        trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))
        apis.handle(req)//处理该请求
      } catch {
        case e: Throwable => error("Exception when handling request", e)
      }
    }
  }

  def shutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)
}

/**
 *
 * @param brokerId 在哪个节点上开启的服务
 * @param requestChannel 每一个节点上有一个socket server,每一个server上对应一个request请求队列
 * @param apis
 * @param numThreads 多少个线程处理这些request队列
 */
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              numThreads: Int) extends Logging with KafkaMetricsGroup {

  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[KafkaRequestHandler](numThreads)
  for(i <- 0 until numThreads) {//根据线程数去开启多个线程
    runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis)
    threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
    threads(i).start()
  }

  def shutdown() {
    info("shutting down")
    for(handler <- runnables)
      handler.shutdown//每个线程发送shutdown命令
    for(thread <- threads)
      thread.join
    info("shut down completely")
  }
}

class BrokerTopicMetrics(name: Option[String]) extends KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] = name match {
    case None => scala.collection.Map.empty
    case Some(topic) => Map("topic" -> topic)
  }

  val messagesInRate = newMeter("MessagesInPerSec", "messages", TimeUnit.SECONDS, tags)
  val bytesInRate = newMeter("BytesInPerSec", "bytes", TimeUnit.SECONDS, tags)
  val bytesOutRate = newMeter("BytesOutPerSec", "bytes", TimeUnit.SECONDS, tags)
  val bytesRejectedRate = newMeter("BytesRejectedPerSec", "bytes", TimeUnit.SECONDS, tags)
  val failedProduceRequestRate = newMeter("FailedProduceRequestsPerSec", "requests", TimeUnit.SECONDS, tags)
  val failedFetchRequestRate = newMeter("FailedFetchRequestsPerSec", "requests", TimeUnit.SECONDS, tags)
}

object BrokerTopicStats extends Logging {
  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  private val allTopicsStats = new BrokerTopicMetrics(None)

  def getBrokerAllTopicsStats(): BrokerTopicMetrics = allTopicsStats

  def getBrokerTopicStats(topic: String): BrokerTopicMetrics = {
    stats.getAndMaybePut(topic)
  }
}
