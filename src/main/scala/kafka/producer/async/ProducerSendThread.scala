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

package kafka.producer.async

import kafka.utils.{SystemTime, Logging}
import java.util.concurrent.{TimeUnit, CountDownLatch, BlockingQueue}
import collection.mutable.ArrayBuffer
import kafka.producer.KeyedMessage
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge

/**
 * 异步生产者线程,该线程用于发送异步生产者,queueTime,batchSize参数来源于AsyncProducerConfig配置文件
 * 
 * 异步的需要开启一个生产者线程,可以批量的发送数据,而不是一条一条发送数据
 */
class ProducerSendThread[K,V](val threadName: String,
                              val queue: BlockingQueue[KeyedMessage[K,V]],//参数存储KeyedMessage信息的队列
                              val handler: EventHandler[K,V],//真正发送信息的处理类,可以批量发送信息
                              val queueTime: Long,//在该队列中最大的缓存时间,超过该时间的数据必须被发送出去
                              val batchSize: Int,//批处理大小
                              val clientId: String) extends Thread(threadName) with Logging with KafkaMetricsGroup {

  private val shutdownLatch = new CountDownLatch(1)
  
  //发送shutdown命令字符串
  private val shutdownCommand = new KeyedMessage[K,V]("shutdown", null.asInstanceOf[K], null.asInstanceOf[V])

  newGauge("ProducerQueueSize",
          new Gauge[Int] {
            def value = queue.size
          },
          Map("clientId" -> clientId))

  override def run {
    try {
      processEvents
    }catch {
      case e: Throwable => error("Error in sending events: ", e)
    }finally {
      shutdownLatch.countDown//运行结束,将其-1,即可以进行shutdown了
    }
  }

  def shutdown = {
    info("Begin shutting down ProducerSendThread")
    queue.put(shutdownCommand)//发送shutdown命令
    shutdownLatch.await//等待一直可以shutdown未知
    info("Shutdown ProducerSendThread complete")
  }

  //真正意义上的处理类
  private def processEvents() {
    var lastSend = SystemTime.milliseconds
    var events = new ArrayBuffer[KeyedMessage[K,V]]
    var full: Boolean = false//设置是否达到了批处理上限

    // drain the queue until you get a shutdown command 一直从queue队列获取信息,直到shutdown命令被取出为止
    //queue.poll(scala.math.max(0, (lastSend + queueTime) - SystemTime.milliseconds), TimeUnit.MILLISECONDS) 表示获取一个元素,最多等待XXX毫秒获取不到就返回null即可
    Stream.continually(queue.poll(scala.math.max(0, (lastSend + queueTime) - SystemTime.milliseconds), TimeUnit.MILLISECONDS))
                      .takeWhile(item => if(item != null) item ne shutdownCommand else true).foreach {
      //currentQueueItem 表示获取的元素KeyedMessage[K, V]
      currentQueueItem =>
        val elapsed = (SystemTime.milliseconds - lastSend)
        // check if the queue time is reached. This happens when the poll method above returns after a timeout and
        // returns a null object
        val expired = currentQueueItem == null
        if(currentQueueItem != null) {//如果获取到数据则,则添加到ArrayBuffer的events中
          trace("Dequeued item for topic %s, partition key: %s, data: %s"
              .format(currentQueueItem.topic, currentQueueItem.key, currentQueueItem.message))
          events += currentQueueItem
        }

        // check if the batch size is reached 设置是否达到了批处理上限
        full = events.size >= batchSize

        //达到了批处理上限或者队列里面已经没有数据了,则发送缓存中的事件集合
        if(full || expired) {
          if(expired)
            debug(elapsed + " ms elapsed. Queue time reached. Sending..")
          if(full)
            debug("Batch full. Sending..")
          // if either queue time has reached or batch size has reached, dispatch to event handler
          tryToHandle(events)
          lastSend = SystemTime.milliseconds
          events = new ArrayBuffer[KeyedMessage[K,V]]
        }
    }
    // send the last batch of events 发送最后一批次事件
    tryToHandle(events)
    if(queue.size > 0)
      throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, %d remaining items in the queue"
        .format(queue.size))
  }

  //真正发送事件的方法,将信息集合发送出去
  def tryToHandle(events: Seq[KeyedMessage[K,V]]) {
    val size = events.size
    try {
      debug("Handling " + size + " events")
      if(size > 0)
        handler.handle(events)
    }catch {
      case e: Throwable => error("Error in handling batch of " + size + " events", e)
    }
  }

}
