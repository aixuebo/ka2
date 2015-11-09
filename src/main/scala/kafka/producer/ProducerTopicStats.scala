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

import kafka.metrics.KafkaMetricsGroup
import kafka.common.{ClientIdTopic, ClientIdAllTopics, ClientIdAndTopic}
import kafka.utils.{Pool, threadsafe}
import java.util.concurrent.TimeUnit


/**
 * 每一个生产者clientId--topic对应一组统计信息
 */
@threadsafe
class ProducerTopicMetrics(metricId: ClientIdTopic) extends KafkaMetricsGroup {
  val tags = metricId match {
    case ClientIdAndTopic(clientId, topic) => Map("clientId" -> clientId, "topic" -> topic)//根据clientId、topic获取统计对象
    case ClientIdAllTopics(clientId) => Map("clientId" -> clientId)//根据clientId获取所有的topic获取统计对象
  }

  //统计信息
  val messageRate = newMeter("MessagesPerSec", "messages", TimeUnit.SECONDS, tags)
  val byteRate = newMeter("BytesPerSec", "bytes", TimeUnit.SECONDS, tags)
  val droppedMessageRate = newMeter("DroppedMessagesPerSec", "drops", TimeUnit.SECONDS, tags)
}

/**
 * Tracks metrics for each topic the given producer client has produced data to.
 * @param clientId The clientId of the given producer client.
 * 每一个clientId生产者对应一个该对象
 * 该对象又按照topic分组了
 */
class ProducerTopicStats(clientId: String) {
  private val valueFactory = (k: ClientIdTopic) => new ProducerTopicMetrics(k)
  private val stats = new Pool[ClientIdTopic, ProducerTopicMetrics](Some(valueFactory))
  //获取该clientId下面所有的topic统计集合
  private val allTopicsStats = new ProducerTopicMetrics(new ClientIdAllTopics(clientId)) // to differentiate from a topic named AllTopics

  //获取该clientId下面所有的topic统计
  def getProducerAllTopicsStats(): ProducerTopicMetrics = allTopicsStats

  //获取该clientId--topic获取统计信息对象
  def getProducerTopicStats(topic: String): ProducerTopicMetrics = {
    stats.getAndMaybePut(new ClientIdAndTopic(clientId, topic))
  }
}

/**
 * Stores the topic stats information of each producer client in a (clientId -> ProducerTopicStats) map.
 * 全局的,为每个clientId生产者绑定一个ProducerTopicStats对象,而ProducerTopicStats对象又按照topic分组了
 */
object ProducerTopicStatsRegistry {
  private val valueFactory = (k: String) => new ProducerTopicStats(k)
  private val globalStats = new Pool[String, ProducerTopicStats](Some(valueFactory))

  def getProducerTopicStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }

  def removeProducerTopicStats(clientId: String) {
    globalStats.remove(clientId)
  }
}
