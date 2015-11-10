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

import scala.collection.mutable
import scala.collection.Set
import scala.collection.Map
import kafka.utils.{Utils, Logging}
import kafka.cluster.Broker
import kafka.metrics.KafkaMetricsGroup
import kafka.common.TopicAndPartition
import com.yammer.metrics.core.Gauge

/**
 * 参数numFetchers,表示多少线程去抓取数据
 */
abstract class AbstractFetcherManager(protected val name: String, clientId: String, numFetchers: Int = 1)
  extends Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher
  //每一个节点和partition对应的抓取线程
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, AbstractFetcherThread]
  private val mapLock = new Object
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
  "MinFetchRate", {
    new Gauge[Double] {
      // current min fetch rate across all fetchers/topics/partitions
      def value = {
        val headRate: Double =
          fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
          fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
        })
      }
    }
  },
  Map("clientId" -> clientId)
  )

  //通过topic-partition,获取第几个线程去抓取该partition数据
  private def getFetcherId(topic: String, partitionId: Int) : Int = {
    Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers
  }

  // to be defined in subclass to create a specific fetcher
  //参数fetcherId表示第几个线程,sourceBroker表示请求的节点,创建一个抓取请求
  def createFetcherThread(fetcherId: Int, sourceBroker: Broker): AbstractFetcherThread

  //为partition添加到抓取线程中
  def addFetcherForPartitions(partitionAndOffsets: Map[TopicAndPartition, BrokerAndInitialOffset]) {
    mapLock synchronized {
      //Map[BrokerAndFetcherId, Map[TopicAndPartition, BrokerAndInitialOffset]] 按照BrokerAndFetcherId对要抓取的partition分组
      val partitionsPerFetcher = partitionAndOffsets.groupBy{ case(topicAndPartition, brokerAndInitialOffset) =>
        BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicAndPartition.topic, topicAndPartition.partition))}
      
      for ((brokerAndFetcherId, partitionAndOffsets) <- partitionsPerFetcher) {
        var fetcherThread: AbstractFetcherThread = null
        //为每一个抓取线程启动一个线程,如果已经存在,则直接返回即可
        fetcherThreadMap.get(brokerAndFetcherId) match {
          case Some(f) => fetcherThread = f
          case None =>
            fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
            fetcherThreadMap.put(brokerAndFetcherId, fetcherThread)
            fetcherThread.start
        }

        //参数topicAndPartition, brokerAndInitOffset,转化成Map,key是TopicAndPartition,value是brokerAndInitOffset.initOffset
        //为该partition分配到抓取线程中
        fetcherThreadMap(brokerAndFetcherId).addPartitions(partitionAndOffsets.map { case (topicAndPartition, brokerAndInitOffset) =>
          topicAndPartition -> brokerAndInitOffset.initOffset
        })
      }
    }

    info("Added fetcher for partitions %s".format(partitionAndOffsets.map{ case (topicAndPartition, brokerAndInitialOffset) =>
      "[" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] "}))
  }

  //不再抓取这些partition,因为这些partition已经是leader了,即该节点不在抓取该partition数据
  def removeFetcherForPartitions(partitions: Set[TopicAndPartition]) {
    mapLock synchronized {
      for ((key, fetcher) <- fetcherThreadMap) {
        fetcher.removePartitions(partitions)
      }
    }
    info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
  }

  //将闲置的抓取线程shutdown,即抓取线程没有任务的,则shutdown
  def shutdownIdleFetcherThreads() {
    mapLock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
      for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {//说明没有任务的线程
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      //统一移除该闲置的抓取线程
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  //关闭每一个抓取线程
  def closeAllFetchers() {
    mapLock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
    }
  }
}

//参数fetcherId表示第几个线程去抓取该broker,因为抓取线程只需要向leader申请抓取即可,因此broker就是partition的leader所在节点
case class BrokerAndFetcherId(broker: Broker, fetcherId: Int)

//该partition对应在leader节点broker,要去抓取的开始位置
case class BrokerAndInitialOffset(broker: Broker, initOffset: Long)