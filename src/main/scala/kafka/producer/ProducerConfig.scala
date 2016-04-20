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

import async.AsyncProducerConfig
import java.util.Properties
import kafka.utils.{Utils, VerifiableProperties}
import kafka.message.{CompressionCodec, NoCompressionCodec}
import kafka.common.{InvalidConfigException, Config}

object ProducerConfig extends Config {
  
  //校验三个属性
  def validate(config: ProducerConfig) {
    validateClientId(config.clientId)
    validateBatchSize(config.batchNumMessages, config.queueBufferingMaxMessages)
    validateProducerType(config.producerType)
  }

  //校验clientId这个值
  def validateClientId(clientId: String) {
    validateChars("client.id", clientId)
  }

  //校验批处理的batchSize 不能大于 队列大小
  def validateBatchSize(batchSize: Int, queueSize: Int) {
    if (batchSize > queueSize)
      throw new InvalidConfigException("Batch size = " + batchSize + " can't be larger than queue size = " + queueSize)
  }

  //校验类型只能是sync或者async
  def validateProducerType(producerType: String) {
    producerType match {
      case "sync" =>
      case "async"=>
      case _ => throw new InvalidConfigException("Invalid value " + producerType + " for producer.type, valid values are sync/async")
    }
  }
}

class ProducerConfig private (val props: VerifiableProperties)
        extends AsyncProducerConfig with SyncProducerConfigShared {
  import ProducerConfig._

  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    props.verify()
  }

  /** This is for bootstrapping and the producer will only use it for getting metadata
   * (topics, partitions and replicas). The socket connections for sending the actual data
   * will be established based on the broker information returned in the metadata. The
   * format is host1:port1,host2:port2, and the list can be a subset of brokers or
   * a VIP pointing to a subset of brokers.
   * 格式HOST1:PORT1,HOST2:PORT2,即生产者要向那些broker发送信息
   */
  val brokerList = props.getString("metadata.broker.list")

  /** the partitioner class for partitioning events amongst sub-topics 选择partition的策略实现类,根据key的hash去魔获取分区*/
  val partitionerClass = props.getString("partitioner.class", "kafka.producer.DefaultPartitioner")

  /** this parameter specifies whether the messages are sent asynchronously *
   * or not. Valid values are - async for asynchronous send                 *
   *                            sync for synchronous send                   */
  val producerType = props.getString("producer.type", "sync")//同步方式

  /**
   * This parameter allows you to specify the compression codec for all data generated *
   * by this producer. The default is NoCompressionCodec
   * 数据上传到broker时候的压缩方式,'none', 'gzip', 'snappy', or 'lz4'
   * 
   */
  val compressionCodec = props.getCompressionCodec("compression.codec", NoCompressionCodec)

  /** This parameter allows you to set whether compression should be turned *
   *  on for particular topics
   *
   *  If the compression codec is anything other than NoCompressionCodec,
   *
   *    Enable compression only for specified topics if any
   *
   *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
   *
   *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
   *  按逗号拆分,返回集合
   */
  val compressedTopics = Utils.parseCsvList(props.getString("compressed.topics", null))

  /** The leader may be unavailable transiently, which can fail the sending of a message.
    *  This property specifies the number of retries when such failures occur.
    *  broker能够在很多理由下,接收信息失败,他们可能仅仅在一段期间内不能接收信息,因此该配置只是了尝试发送次数,超过了该次数,则该信息要被丢弃或者其他处理
    */
  val messageSendMaxRetries = props.getInt("message.send.max.retries", 3)

  /** Before each retry, the producer refreshes the metadata of relevant topics. Since leader
    * election takes a bit of time, this property specifies the amount of time that the producer
    * waits before refreshing the metadata.
    * 每一次失败后,要sleep多少秒后再进行尝试
    */
  val retryBackoffMs = props.getInt("retry.backoff.ms", 100)

  /**
   * The producer generally refreshes the topic metadata from brokers when there is a failure
   * (partition missing, leader not available...). It will also poll regularly (default: every 10min
   * so 600000ms). If you set this to a negative value, metadata will only get refreshed on failure.
   * If you set this to zero, the metadata will get refreshed after each message sent (not recommended)
   * Important note: the refresh happen only AFTER the message is sent, so if the producer never sends
   * a message the metadata is never refreshed
   * topic的元数据刷新时间间隔
   */
  val topicMetadataRefreshIntervalMs = props.getInt("topic.metadata.refresh.interval.ms", 600000)

  validate(this)
}
