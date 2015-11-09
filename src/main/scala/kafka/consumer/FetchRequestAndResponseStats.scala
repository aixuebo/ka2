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

import java.util.concurrent.TimeUnit

import kafka.common.{ClientIdAllBrokers, ClientIdBroker, ClientIdAndBroker}
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.Pool

//用于 统计  抓取的请求和返回值 信息

//该class表示具体的统计信息
class FetchRequestAndResponseMetrics(metricId: ClientIdBroker) extends KafkaMetricsGroup {
  val tags = metricId match {
    case ClientIdAndBroker(clientId, brokerHost, brokerPort) =>
      Map("clientId" -> clientId, "brokerHost" -> brokerHost,
      "brokerPort" -> brokerPort.toString)
    case ClientIdAllBrokers(clientId) =>
      Map("clientId" -> clientId)
  }

  val requestTimer = new KafkaTimer(newTimer("FetchRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags))
  val requestSizeHist = newHistogram("FetchResponseSize", biased = true, tags)
}

/**
 * Tracks metrics of the requests made by a given consumer client to all brokers, and the responses obtained from the brokers.
 * @param clientId ClientId of the given consumer
 * 统计对象,每一个客户端clientId对应一个该对象
 */
class FetchRequestAndResponseStats(clientId: String) {
  private val valueFactory = (k: ClientIdBroker) => new FetchRequestAndResponseMetrics(k)
  
  //clientId, brokerHost, brokerPort 对应一个FetchRequestAndResponseMetrics对象,可以统计细分到该客户端请求哪些host、port节点的信息
  private val stats = new Pool[ClientIdBroker, FetchRequestAndResponseMetrics](Some(valueFactory))
  //clientId对应一个FetchRequestAndResponseMetrics对象,可以统计该客户端总体的信息
  private val allBrokersStats = new FetchRequestAndResponseMetrics(new ClientIdAllBrokers(clientId))

  //获取clientId 对应一个FetchRequestAndResponseMetrics对象
  def getFetchRequestAndResponseAllBrokersStats(): FetchRequestAndResponseMetrics = allBrokersStats

  //获取clientId, brokerHost, brokerPort 对应一个FetchRequestAndResponseMetrics对象
  def getFetchRequestAndResponseStats(brokerHost: String, brokerPort: Int): FetchRequestAndResponseMetrics = {
    stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort))
  }
}

/**
 * Stores the fetch request and response stats information of each consumer client in a (clientId -> FetchRequestAndResponseStats) map.
 */
object FetchRequestAndResponseStatsRegistry {
  private val valueFactory = (k: String) => new FetchRequestAndResponseStats(k)
  
  //每一个客户端消费者clientId,对应一个统计对象FetchRequestAndResponseStats
  private val globalStats = new Pool[String, FetchRequestAndResponseStats](Some(valueFactory))

  //获取该clientId对应的统计对象FetchRequestAndResponseStats
  def getFetchRequestAndResponseStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }

  //移除该客户端的统计对象
  def removeConsumerFetchRequestAndResponseStats(clientId: String) {
    val pattern = (".*" + clientId + ".*").r
    val keys = globalStats.keys
    for (key <- keys) {
      pattern.findFirstIn(key) match {
        case Some(_) => globalStats.remove(key)
        case _ =>
      }
    }
  }
}


