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

import kafka.cluster.Broker
import kafka.utils.{Pool, ShutdownableThread}
import kafka.consumer.{PartitionTopicInfo, SimpleConsumer}
import kafka.api.{FetchRequest, FetchResponse, FetchResponsePartitionData, FetchRequestBuilder}
import kafka.common.{KafkaException, ClientIdAndBroker, TopicAndPartition, ErrorMapping}
import kafka.utils.Utils.inLock
import kafka.message.{InvalidMessageException, ByteBufferMessageSet, MessageAndOffset}
import kafka.metrics.KafkaMetricsGroup

import scala.collection.{mutable, Set, Map}
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 *  消费者clientId,要向sourceBroker该节点发送请求,获取数据
 *  sourceBroker节点就是partition的leader节点
 */

/**
 * @fetchSize 表示每次抓取的数据数量
 * @fetcherBrokerId 标示本地消费者节点
 */
abstract class AbstractFetcherThread(name: String, clientId: String, sourceBroker: Broker, socketTimeout: Int, socketBufferSize: Int,
                                     fetchSize: Int, fetcherBrokerId: Int = -1, maxWait: Int = 0, minBytes: Int = 1,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {
  //等待抓取的数据映射
  private val partitionMap = new mutable.HashMap[TopicAndPartition, Long] // a (topic, partition) -> offset map
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()
  //产生一个消费者,即该消费者消费数据,因为sourceBroker节点就是partition的leader节点
  val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize, clientId)
  //客户端向host:prot的标示
  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)
  
  //抓取请求生成器
  val fetchRequestBuilder = new FetchRequestBuilder().
          clientId(clientId).
          replicaId(fetcherBrokerId).//消费者节点
          maxWait(maxWait).
          minBytes(minBytes)

  /* callbacks to be defined in subclass */

  // process fetched data 处理抓取数据
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long,
                           partitionData: FetchResponsePartitionData)

  // handle a partition whose offset is out of range and return a new fetch offset 出去抓取的位置超过范围的请求
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long

  // deal with partitions with errors, potentially due to leadership changes 处理抓取过程中出现异常的情况
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition])

  override def shutdown(){
    super.shutdown()
    simpleConsumer.close()
  }

  override def doWork() {
    inLock(partitionMapLock) {
      if (partitionMap.isEmpty)
        partitionMapCond.await(200L, TimeUnit.MILLISECONDS)
        
      //添加要抓取topic-partition上从offset开始,抓取fetchSize个数据
      partitionMap.foreach {
        case((topicAndPartition, offset)) =>
          fetchRequestBuilder.addFetch(topicAndPartition.topic, topicAndPartition.partition,
                           offset, fetchSize)
      }
    }

    val fetchRequest = fetchRequestBuilder.build()
    
    //真正发送抓取数据请求,去进行抓取数据
    if (!fetchRequest.requestInfo.isEmpty)
      processFetchRequest(fetchRequest)
  }

  //真正发送抓取数据请求,去进行抓取数据
  private def processFetchRequest(fetchRequest: FetchRequest) {
    val partitionsWithError = new mutable.HashSet[TopicAndPartition]//抓取失败的TopicAndPartition集合
    var response: FetchResponse = null
    try {
      //向brokerID 发送请求fetchRequest
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      response = simpleConsumer.fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning.get) {
          warn("Error in fetch %s. Possible cause: %s".format(fetchRequest, t.toString))
          partitionMapLock synchronized {
            partitionsWithError ++= partitionMap.keys
          }
        }
    }
    fetcherStats.requestRate.mark()

    //抓取成功后,处理抓取返回的数据
    if (response != null) {
      // process fetched data
      inLock(partitionMapLock) {
        response.data.foreach {
          case(topicAndPartition, partitionData) =>
            val (topic, partitionId) = topicAndPartition.asTuple
            val currentOffset = partitionMap.get(topicAndPartition)
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
            //校验请求前时,抓取该partition的开始位置和当前接收到返回值后的位置是否一致,进入if说明是一致的,即合法的
            if (currentOffset.isDefined && fetchRequest.requestInfo(topicAndPartition).offset == currentOffset.get) {
              partitionData.error match {
                case ErrorMapping.NoError =>
                  //说明抓取数据成功
                  try {
                    val messages = partitionData.messages.asInstanceOf[ByteBufferMessageSet]
                    val validBytes = messages.validBytes
                    //获取抓取后的更新位置
                    val newOffset = messages.shallowIterator.toSeq.lastOption match {
                      case Some(m: MessageAndOffset) => m.nextOffset
                      case None => currentOffset.get
                    }
                    //进行更新,表示已经抓取该partition的newOffset位置了,下次抓取从该位置开始抓取
                    partitionMap.put(topicAndPartition, newOffset)
                    fetcherLagStats.getFetcherLagStats(topic, partitionId).lag = partitionData.hw - newOffset
                    fetcherStats.byteRate.mark(validBytes)
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread 处理抓取回来的数据
                    processPartitionData(topicAndPartition, currentOffset.get, partitionData)
                  } catch {
                    case ime: InvalidMessageException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      //    should get fixed in the subsequent fetches
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentOffset.get + " error " + ime.getMessage)
                    case e: Throwable =>
                      throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                                               .format(topic, partitionId, currentOffset.get), e)
                  }
                case ErrorMapping.OffsetOutOfRangeCode =>
                  try {
                    //超出了获取数据的位置,要重新计算位置
                    val newOffset = handleOffsetOutOfRange(topicAndPartition)
                    partitionMap.put(topicAndPartition, newOffset)
                    error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                      .format(currentOffset.get, topic, partitionId, newOffset))
                  } catch {
                    case e: Throwable =>
                      error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      partitionsWithError += topicAndPartition
                  }
                case _ =>
                  if (isRunning.get) {
                    error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                      ErrorMapping.exceptionFor(partitionData.error).getClass))
                    partitionsWithError += topicAndPartition
                  }
              }
            }
        }
      }
    }

    if(partitionsWithError.size > 0) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  //为该线程添加要抓取的partition集合,key是partition对象,value是从什么节点开始同步
  def addPartitions(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      for ((topicAndPartition, offset) <- partitionAndOffsets) {
        // If the partitionMap already has the topic/partition, then do not update the map with the old offset
        if (!partitionMap.contains(topicAndPartition))
          partitionMap.put(
            topicAndPartition,
            if (PartitionTopicInfo.isOffsetInvalid(offset)) handleOffsetOutOfRange(topicAndPartition) else offset)
      }
      partitionMapCond.signalAll()
    } finally {
      partitionMapLock.unlock()
    }
  }

  //不在抓取该partition
  def removePartitions(topicAndPartitions: Set[TopicAndPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicAndPartitions.foreach(tp => partitionMap.remove(tp))
    } finally {
      partitionMapLock.unlock()
    }
  }

  //正在抓取的partition数量
  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try {
      partitionMap.size
    } finally {
      partitionMapLock.unlock()
    }
  }
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {
  private[this] val lagVal = new AtomicLong(-1L)
  newGauge("ConsumerLag",
    new Gauge[Long] {
      def value = lagVal.get
    },
    Map("clientId" -> metricId.clientId,
      "topic" -> metricId.topic,
      "partition" -> metricId.partitionId.toString)
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getFetcherLagStats(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter("BytesPerSec", "bytes", TimeUnit.SECONDS, tags)
}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

