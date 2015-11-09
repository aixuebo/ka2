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

import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import java.util.concurrent.TimeUnit
import kafka.utils.Pool
import kafka.common.{ClientIdAllBrokers, ClientIdBroker, ClientIdAndBroker}

//clientId-brokerHost-brokerPort,表示标示了生产者clientId向哪个broker节点的哪个端口进行发送的信息的统计信息
class ProducerRequestMetrics(metricId: ClientIdBroker) extends KafkaMetricsGroup {
  val tags = metricId match {
    case ClientIdAndBroker(clientId, brokerHost, brokerPort) => Map("clientId" -> clientId, "brokerHost" -> brokerHost, "brokerPort" -> brokerPort.toString)
    case ClientIdAllBrokers(clientId) => Map("clientId" -> clientId)
  }

  //用于统计请求时间
  val requestTimer = new KafkaTimer(newTimer("ProducerRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags))
  
  //用于统计请求的字节总数
  val requestSizeHist = newHistogram("ProducerRequestSize", biased = true, tags)
}

/**
 * Tracks metrics of requests made by a given producer client to all brokers.
 * @param clientId ClientId of the given producer
 */
class ProducerRequestStats(clientId: String) {
  private val valueFactory = (k: ClientIdBroker) => new ProducerRequestMetrics(k)
  private val stats = new Pool[ClientIdBroker, ProducerRequestMetrics](Some(valueFactory))//clientId-brokerHost-brokerPort,表示标示了生产者clientId向哪个broker节点的哪个端口进行发送的信息
  private val allBrokersStats = new ProducerRequestMetrics(new ClientIdAllBrokers(clientId))//clientId-AllBrokers,表示仅仅标示生产者clientId,而不管他向哪个broker节点发送信息

  def getProducerRequestAllBrokersStats(): ProducerRequestMetrics = allBrokersStats

  //clientId-brokerHost-brokerPort,表示标示了生产者clientId向哪个broker节点的哪个端口进行发送的信息
  def getProducerRequestStats(brokerHost: String, brokerPort: Int): ProducerRequestMetrics = {
    stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort))
  }
}

/**
 * Stores the request stats information of each producer client in a (clientId -> ProducerRequestStats) map.
 * clientId-brokerHost-brokerPort,表示标示了生产者clientId向哪个broker节点的哪个端口进行发送的信息
 */
object ProducerRequestStatsRegistry {
  private val valueFactory = (k: String) => new ProducerRequestStats(k)
  private val globalStats = new Pool[String, ProducerRequestStats](Some(valueFactory))

  def getProducerRequestStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }

  def removeProducerRequestStats(clientId: String) {
    globalStats.remove(clientId)
  }
}

