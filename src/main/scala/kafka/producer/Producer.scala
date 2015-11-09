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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.common.{AppInfo, QueueFullException}
import kafka.metrics._
import kafka.producer.async.{DefaultEventHandler, EventHandler, ProducerSendThread}
import kafka.serializer.Encoder
import kafka.utils._


class Producer[K,V](val config: ProducerConfig,
                    private val eventHandler: EventHandler[K,V])  // only for unit testing
  extends Logging {

  private val hasShutdown = new AtomicBoolean(false)
  
  //生产者缓存信息的队列
  private val queue = new LinkedBlockingQueue[KeyedMessage[K,V]](config.queueBufferingMaxMessages)

  private var sync: Boolean = true//是否是同步
  private var producerSendThread: ProducerSendThread[K,V] = null//异步生产者时,需要一个发送线程
  private val lock = new Object()

  config.producerType match {
    case "sync" =>
    case "async" =>//异步的需要开启一个生产者线程,可以批量的发送数据,而不是一条一条发送数据
      sync = false
      producerSendThread = new ProducerSendThread[K,V]("ProducerSendThread-" + config.clientId,
                                                       queue,
                                                       eventHandler,
                                                       config.queueBufferingMaxMs,
                                                       config.batchNumMessages,
                                                       config.clientId)
      producerSendThread.start()
  }

  private val producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId)

  KafkaMetricsReporter.startReporters(config.props)
  AppInfo.registerInfo()

  def this(config: ProducerConfig) =
    this(config,
         new DefaultEventHandler[K,V](config,
                                      Utils.createObject[Partitioner](config.partitionerClass, config.props),
                                      Utils.createObject[Encoder[V]](config.serializerClass, config.props),
                                      Utils.createObject[Encoder[K]](config.keySerializerClass, config.props),
                                      new ProducerPool(config)))

  /**
   * Sends the data, partitioned by key to the topic using either the
   * synchronous or the asynchronous producer
   * @param messages the producer data object that encapsulates the topic, key and message data
   */
  def send(messages: KeyedMessage[K,V]*) {
    lock synchronized {
      if (hasShutdown.get)
        throw new ProducerClosedException
      recordStats(messages)//统计数据信息
      sync match {//发送数据
        case true => eventHandler.handle(messages)//同步发送
        case false => asyncSend(messages)//异步发送
      }
    }
  }

  //统计数据信息
  private def recordStats(messages: Seq[KeyedMessage[K,V]]) {
    for (message <- messages) {
      producerTopicStats.getProducerTopicStats(message.topic).messageRate.mark()
      producerTopicStats.getProducerAllTopicsStats.messageRate.mark()
    }
  }

  //处理异步请求发送数据
  private def asyncSend(messages: Seq[KeyedMessage[K,V]]) {
    for (message <- messages) {
      val added = config.queueEnqueueTimeoutMs match {
        case 0  =>
          queue.offer(message)//直接进入队列
        case _  =>
          try {
            config.queueEnqueueTimeoutMs < 0 match {
            case true =>//进入阻塞队列
              queue.put(message)
              true
            case _ =>//队列进入阻塞队列,阻塞事件为N毫秒
              queue.offer(message, config.queueEnqueueTimeoutMs, TimeUnit.MILLISECONDS)
            }
          }
          catch {
            case e: InterruptedException =>
              false
          }
      }
      if(!added) {//如果事件向队列中添加失败,记录统计信息,并且抛异常
        producerTopicStats.getProducerTopicStats(message.topic).droppedMessageRate.mark()
        producerTopicStats.getProducerAllTopicsStats.droppedMessageRate.mark()
        throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + message.toString)
      }else {//添加成功打印日志
        trace("Added to send queue an event: " + message.toString)
        trace("Remaining queue size: " + queue.remainingCapacity)
      }
    }
  }

  /**
   * Close API to close the producer pool connections to all Kafka brokers. Also closes
   * the zookeeper client connection if one exists
   */
  def close() = {
    lock synchronized {
      //关闭是先将hasShutdown从false设置成true
      val canShutdown = hasShutdown.compareAndSet(false, true)
      if(canShutdown) {//设置true后,调用异步线程的shutdown、以及调用eventHandler.close方法
        info("Shutting down producer")
        val startTime = System.nanoTime()
        KafkaMetricsGroup.removeAllProducerMetrics(config.clientId)
        if (producerSendThread != null)
          producerSendThread.shutdown
        eventHandler.close
        info("Producer shutdown completed in " + (System.nanoTime() - startTime) / 1000000 + " ms")
      }
    }
  }
}


