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

import kafka.cluster.Broker
import kafka.server.AbstractFetcherThread
import kafka.message.ByteBufferMessageSet
import kafka.api.{Request, OffsetRequest, FetchResponsePartitionData}
import kafka.common.TopicAndPartition

/**
 * 消费者真正抓取数据的线程
 * 他会去真正去一个节点去抓取数据,抓取后存储在对应topic-partition的队列中,未后续消费者消费message信息
 */
class ConsumerFetcherThread(name: String,//线程名字
                            val config: ConsumerConfig,
                            sourceBroker: Broker,//去哪个节点去抓取数据
                            partitionMap: Map[TopicAndPartition, PartitionTopicInfo],//消费者端为每一个topic-partition生成一个该对象,描述抓取和消费的情况以及抓取回来存储的队列
                            val consumerFetcherManager: ConsumerFetcherManager)//该类统一对所有的抓取线程进行管理
        extends AbstractFetcherThread(name = name, 
                                      clientId = config.clientId,//指明是一个kafka的消费者客户端,被使用与区分不同的客户端
                                      sourceBroker = sourceBroker,//去哪个节点去抓取数据
                                      socketTimeout = config.socketTimeoutMs,//数据发送的socket超时时间
                                      socketBufferSize = config.socketReceiveBufferBytes,//数据发送的socket的缓冲大小
                                      fetchSize = config.fetchMessageMaxBytes,//一次抓取message的最大字节
                                      fetcherBrokerId = Request.OrdinaryConsumerId,//表示抓取的节点ID,如果是纯粹的客户端消费者,则-1即可,如果是集群上其他follow节点抓取数据,则该字段是有值的
                                      maxWait = config.fetchWaitMaxMs,//如果没有充足的数据去立即满足抓取的最小值,则在返回给抓取请求客户端之前,在server对岸最大的停留时间
                                      minBytes = config.fetchMinBytes,//一次抓取请求,server最小返回给客户端的字节数,如果请求不足这些数据,则请求将会被阻塞
                                      isInterruptible = true) {//如果是true,表示停止后要调用interrupt方法

  /**
   * process fetched data处理已经抓取到的信息
   * @param topicAndPartition 刚刚为该topic-partition抓取到了数据
   * @param fetchOffset 刚刚抓取到的第一个序号
   * @param partitionData 真正抓取到的数据
   */
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: FetchResponsePartitionData) {
    val pti = partitionMap(topicAndPartition)//找到该topic-partition对应的队列对象
    if (pti.getFetchOffset != fetchOffset)//说明下一个要抓取的序号与现在抓取的序号不匹配
      throw new RuntimeException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d"
                                .format(topicAndPartition.topic, topicAndPartition.partition, pti.getFetchOffset, fetchOffset))//说明不匹配,要抛异常
    pti.enqueue(partitionData.messages.asInstanceOf[ByteBufferMessageSet])//将抓取的信息存放到队列中
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  //返回重新设置后的要抓取的偏移量
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    //设置消费的时间,默认从最新的数据开始消费
    var startTimestamp : Long = 0
    config.autoOffsetReset match {//消费者从topic的什么位置开始消费,从头还是消费最新的,默认是消费最新的
      case OffsetRequest.SmallestTimeString => startTimestamp = OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => startTimestamp = OffsetRequest.LatestTime
      case _ => startTimestamp = OffsetRequest.LatestTime //默认是消费最新的位置
    }
    //查找最新的序号
    val newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId)
    val pti = partitionMap(topicAndPartition)//找到topic-partition对应的队列对象
    //设置该队列对象的下一个抓取的序号以及已经消费到什么序号位置了
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  //进入该方法,说明在抓取的过程中,该topic-partition集合抓取失败了
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    removePartitions(partitions.toSet)//移除这些失败的topic-partition,即不在抓取这些数据了,注意:只是暂时不抓取已经等待抓取的topic-partition,未来再添加到抓取线程的,还是要进行抓取的
    consumerFetcherManager.addPartitionsWithError(partitions)//通知上一级,说这些topic-partition抓取失败
  }
}
