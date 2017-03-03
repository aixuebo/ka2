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

import kafka.utils.{IteratorTemplate, Logging, Utils}
import java.util.concurrent.{TimeUnit, BlockingQueue}
import kafka.serializer.Decoder
import java.util.concurrent.atomic.AtomicReference
import kafka.message.{MessageAndOffset, MessageAndMetadata}
import kafka.common.{KafkaException, MessageSizeTooLargeException}


/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * 该迭代器是阻塞的,直到有一个value值被返回为止
 * The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
 * 这个迭代其可以拿到一个shutdown命令,去添加到队列进行shutdown处理
 */
class ConsumerIterator[K, V](private val channel: BlockingQueue[FetchedDataChunk],//队列存储抓取的数据,该消费者迭代器就消费这些数据
                             consumerTimeoutMs: Int,//因为在一定时间内没有从队列中获取数据,超时了,所以要抛异常
                             private val keyDecoder: Decoder[K],//对数据中key进行重新编码
                             private val valueDecoder: Decoder[V],//对数据中value进行重新编码
                             val clientId: String) //客户端的唯一识别码,有时候也是groupId
  extends IteratorTemplate[MessageAndMetadata[K, V]] with Logging {

  private var current: AtomicReference[Iterator[MessageAndOffset]] = new AtomicReference(null)//就是MessageAndOffset的迭代器
  private var currentTopicInfo: PartitionTopicInfo = null//当前的topic-partition的一些信息
  private var consumedOffset: Long = -1L//消费者下一个要消费的message序号
  private val consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId)//统计信息

  override def next(): MessageAndMetadata[K, V] = {
    val item = super.next()

    //设置一些额外的信息和统计
    if(consumedOffset < 0)
      throw new KafkaException("Offset returned by the message set is invalid %d".format(consumedOffset))
    //设置一下下一个要消费哪个序号的信息
    currentTopicInfo.resetConsumeOffset(consumedOffset)
    val topic = currentTopicInfo.topic
    trace("Setting %s consumed offset to %d".format(topic, consumedOffset))
    consumerTopicStats.getConsumerTopicStats(topic).messageRate.mark()
    consumerTopicStats.getConsumerAllTopicStats().messageRate.mark()
    item
  }

  //获取下一个message信息
  protected def makeNext(): MessageAndMetadata[K, V] = {
    var currentDataChunk: FetchedDataChunk = null//当前队列中在处理的元素
    // if we don't have an iterator, get one
    var localCurrent = current.get()
    if(localCurrent == null || !localCurrent.hasNext) {//如果迭代器是null或者没有下一个元素了
      //从队列中获取一个元素
      if (consumerTimeoutMs < 0)//阻塞访问
        currentDataChunk = channel.take//阻塞从队列中获取一个新的messageBuffer对象
      else {
        currentDataChunk = channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS)//可以从队列中等待一会,最终也是获取一个新的messageBuffer对象
        if (currentDataChunk == null) {//说明没有数据了
          // reset state to make the iterator re-iterable 让迭代器暂时没准备好,没准备好的意思就是下次还是会走makeNext方法
          resetState()
          throw new ConsumerTimeoutException//因为在一定时间内没有从队列中获取数据,超时了,所以要抛异常
        }
      }
      if(currentDataChunk eq ZookeeperConsumerConnector.shutdownCommand) {//如果是shutdown命令,则停止迭代
        debug("Received the shutdown command")
        return allDone
      } else {
        currentTopicInfo = currentDataChunk.topicInfo
        val cdcFetchOffset = currentDataChunk.fetchOffset//要去抓取的序号
        val ctiConsumeOffset = currentTopicInfo.getConsumeOffset//已经消费到哪个序号了
        if (ctiConsumeOffset < cdcFetchOffset) {//要去抓取的序号比已经消费的序号该高,说明有数据丢失了
          error("consumed offset: %d doesn't match fetch offset: %d for %s;\n Consumer may lose data"
            .format(ctiConsumeOffset, cdcFetchOffset, currentTopicInfo))
          currentTopicInfo.resetConsumeOffset(cdcFetchOffset)//重新设置已经消费的序号是要抓取的序号,因为丢失了就丢失吧
        }
        localCurrent = currentDataChunk.messages.iterator//迭代该messageBuffer中的message信息

        current.set(localCurrent)//设置当前的message迭代器
      }
      // if we just updated the current chunk and it is empty that means the fetch size is too small!
      //说明这个信息有问题,则抛异常
      if(currentDataChunk.messages.validBytes == 0)
        throw new MessageSizeTooLargeException("Found a message larger than the maximum fetch size of this consumer on topic " +
                                               "%s partition %d at fetch offset %d. Increase the fetch size, or decrease the maximum message size the broker will allow."
                                               .format(currentDataChunk.topicInfo.topic, currentDataChunk.topicInfo.partitionId, currentDataChunk.fetchOffset))
    }
    var item = localCurrent.next()//获取下一个message信息
    // reject the messages that have already been consumed//拒绝已经消费过的信息

    //如果当前的message序号是消费过的,并且message还有信息,我们则看下一个message的信息,这条消费过的忽略掉
    while (item.offset < currentTopicInfo.getConsumeOffset && localCurrent.hasNext) {
      item = localCurrent.next()
    }

    //TODO
    //好像有一个bug,就是当这组消费的都没有满足条件的,都是已经消费过的数据,他还是会把最后一个message给重新消费一遍,然后从队列中获取接下来的要消费的信息
    //这个bug是不是最终的bug,要看BlockingQueue[FetchedDataChunk]队列中的FetchedDataChunk对象是怎么生成的了,以及FetchedDataChunk对象中consumedOffset和fetchedOffset具体含义
    consumedOffset = item.nextOffset//记录消费者下一个要消费的message序号

    item.message.ensureValid() // validate checksum of message to ensure it is valid 进行校验和计算,确保接受的message是正确的

    //返回该message的全部元数据信息
    new MessageAndMetadata(currentTopicInfo.topic, currentTopicInfo.partitionId, item.message, item.offset, keyDecoder, valueDecoder)
  }

  def clearCurrentChunk() {
    try {
      debug("Clearing the current data chunk for this consumer iterator")
      current.set(null)
    }
  }
}

class ConsumerTimeoutException() extends RuntimeException()

