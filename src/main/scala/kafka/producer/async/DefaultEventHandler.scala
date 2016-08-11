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

import kafka.common._
import kafka.message.{NoCompressionCodec, Message, ByteBufferMessageSet}
import kafka.producer._
import kafka.serializer.Encoder
import kafka.utils.{Utils, Logging, SystemTime}
import scala.util.Random
import scala.collection.{Seq, Map}
import scala.collection.mutable.{ArrayBuffer, HashMap, Set}
import java.util.concurrent.atomic._
import kafka.api.{TopicMetadata, ProducerRequest}

/**
 * 生产者将事件发送出去的处理类
 *
 * 流程
 * 1.获取要发送的数据topic-partition--待发送数据集合
 * 2.根据topic获取集群中对应在哪些节点上有partition以及多少个partition
 * 3.从partition集合中分配一个partition的leader,向该leader写入数据
 * 4.将写入同一个节点的所有topic-partition进行汇总
 * 5.对同一个节点的同一个topic-partition的信息集合进行压缩
 * 6.发送数据到指定节点,等待返回信息
 */
class DefaultEventHandler[K,V](config: ProducerConfig,
                               private val partitioner: Partitioner,//该生产者如何分配key到不同的partition中
                               private val encoder: Encoder[V],//value的编码类
                               private val keyEncoder: Encoder[K],//key的编码类
                               private val producerPool: ProducerPool,//与各个节点通信的对象,可以往各个节点的leader所在节点发送信息
                               private val topicPartitionInfos: HashMap[String, TopicMetadata] = new HashMap[String, TopicMetadata]) //用于存储该节点从集群上抓去的每一个topic-partition的详细信息的缓存版本

  extends EventHandler[K,V] with Logging {
  val isSync = ("sync" == config.producerType)//是否是同步生产者

  val correlationId = new AtomicInteger(0)//相关联的ID
  val brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos) //该对象用于去集群抓去topic对应哪些partiion的详细信息,并且加入到缓存集合中

  //定期刷新topic元数据相关参数
  private val topicMetadataRefreshInterval = config.topicMetadataRefreshIntervalMs//刷新topic元数据的周期
  private var lastTopicMetadataRefreshTime = 0L//上一次刷新topic元数据的时间
  private val topicMetadataToRefresh = Set.empty[String]//被刷新的topic元数据集合,有效期是一个topicMetadataRefreshInterval时间周期内有效
  
  private val sendPartitionPerTopicCache = HashMap.empty[String, Int]

  private val producerStats = ProducerStatsRegistry.getProducerStats(config.clientId)//每一个生产者clientId对应一个该对象,该对象作为该生产者的一些统计信息
  private val producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId)//每一个生产者clientId--topic对应一组统计信息

  //发送一组topic-key-value信息
  def handle(events: Seq[KeyedMessage[K,V]]) {
    //对每一个事件的key-value进行字节码转换,转换成字节数组组装的Message对象
    val serializedData = serialize(events)
    serializedData.foreach {
      keyed =>
        val dataSize = keyed.message.payloadSize//获取value的字节长度
        producerTopicStats.getProducerTopicStats(keyed.topic).byteRate.mark(dataSize)
        producerTopicStats.getProducerAllTopicsStats.byteRate.mark(dataSize)
    }
    
    var outstandingProduceRequests = serializedData//等待发送出去的请求集合
    var remainingRetries = config.messageSendMaxRetries + 1//最大尝试次数
    val correlationIdStart = correlationId.get()//获取自增长ID
    debug("Handling %d events".format(events.size))//记录本次要处理多少个事件
    
    while (remainingRetries > 0 && outstandingProduceRequests.size > 0) {//不断尝试发送数据
      topicMetadataToRefresh ++= outstandingProduceRequests.map(_.topic)//获取这些数据中获取topic集合
      
      //将topic元数据信息清空
      if (topicMetadataRefreshInterval >= 0 &&
          SystemTime.milliseconds - lastTopicMetadataRefreshTime > topicMetadataRefreshInterval) {//时间间隔超时,则重新更新topic元数据信息
        Utils.swallowError(brokerPartitionInfo.updateInfo(topicMetadataToRefresh.toSet, correlationId.getAndIncrement)) //对topicMetadataToRefresh的topic集合进行更新partition的内容
        sendPartitionPerTopicCache.clear()
        topicMetadataToRefresh.clear //清空所有topic集合
        lastTopicMetadataRefreshTime = SystemTime.milliseconds //更新最后刷新topic元数据时间戳
      }
      
      outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests)
      if (outstandingProduceRequests.size > 0) {
        info("Back off for %d ms before retrying send. Remaining retries = %d".format(config.retryBackoffMs, remainingRetries-1))
        // back off and update the topic metadata cache before attempting another send operation
        Thread.sleep(config.retryBackoffMs) //尝试失败后,休息一定时间
        // get topics of the outstanding produce requests and refresh metadata for those
        Utils.swallowError(brokerPartitionInfo.updateInfo(outstandingProduceRequests.map(_.topic).toSet, correlationId.getAndIncrement)) //更新这些topic的partition信息
        sendPartitionPerTopicCache.clear()
        remainingRetries -= 1 //尝试次数减一
        producerStats.resendRate.mark()
      }
    }
    
    //如果失败次数超过一定限制,则抛异常
    if(outstandingProduceRequests.size > 0) {
      producerStats.failedSendRate.mark()
      val correlationIdEnd = correlationId.get()
      error("Failed to send requests for topics %s with correlation ids in [%d,%d]"
        .format(outstandingProduceRequests.map(_.topic).toSet.mkString(","),
        correlationIdStart, correlationIdEnd-1))
      throw new FailedToSendMessageException("Failed to send messages after " + config.messageSendMaxRetries + " tries.", null)//打印日志说超过了xxx次最大限制后,依然发送信息失败
    }
  }

  //返回发送未成功的信息集合
  private def dispatchSerializedData(messages: Seq[KeyedMessage[K,Message]]): Seq[KeyedMessage[K, Message]] = {
    //将要发送的信息,按照partition所在的leader节点不同,进行分组
    //返回值Map[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
    //key是partition的leader所在节点,value是向该节点发送哪些topic-partition以及对应的信息
    val partitionedDataOpt = partitionAndCollate(messages)
    partitionedDataOpt match {
      case Some(partitionedData) =>
        val failedProduceRequests = new ArrayBuffer[KeyedMessage[K,Message]]
        try {
          for ((brokerid, messagesPerBrokerMap) <- partitionedData) {
            if (logger.isTraceEnabled)
              messagesPerBrokerMap.foreach(partitionAndEvent =>
                trace("Handling event for Topic: %s, Broker: %d, Partitions: %s".format(partitionAndEvent._1, brokerid, partitionAndEvent._2)))

            //将每一个topic-partition的KeyedMessage集合转换成ByteBufferMessageSet对象,并且该对象可能会被压缩处理了,减少发送数据量
            val messageSetPerBroker = groupMessagesToSet(messagesPerBrokerMap)

            val failedTopicPartitions = send(brokerid, messageSetPerBroker) //向该节点发送信息,返回没有发送成功的数据

            //将没有发送成功的,添加到集合中
            failedTopicPartitions.foreach(topicPartition => {
              messagesPerBrokerMap.get(topicPartition) match {
                case Some(data) => failedProduceRequests.appendAll(data)
                case None => // nothing
              }
            })
          }
        } catch {
          case t: Throwable => error("Failed to send messages", t)
        }
        failedProduceRequests //返回未发送的信息集合
      case None => // all produce requests failed 说明全部信息都不应该被发送出去,因此返回全部信息
        messages
    }
  }

  //对每一个事件的key-value进行字节码转换,转换成字节数组组装的Message对象
  def serialize(events: Seq[KeyedMessage[K,V]]): Seq[KeyedMessage[K,Message]] = {
    val serializedMessages = new ArrayBuffer[KeyedMessage[K,Message]](events.size)
    events.foreach{e =>
      try {
        if(e.hasKey)
          serializedMessages += new KeyedMessage[K,Message](topic = e.topic, key = e.key, partKey = e.partKey, message = new Message(key = keyEncoder.toBytes(e.key), bytes = encoder.toBytes(e.message)))
        else
          serializedMessages += new KeyedMessage[K,Message](topic = e.topic, key = e.key, partKey = e.partKey, message = new Message(bytes = encoder.toBytes(e.message)))
      } catch {
        case t: Throwable =>
          producerStats.serializationErrorRate.mark()
          if (isSync) {
            throw t
          } else {
            // currently, if in async mode, we just log the serialization error. We need to revisit
            // this when doing kafka-496
            error("Error serializing message for topic %s".format(e.topic), t)
          }
      }
    }
    serializedMessages
  }

  /**
   * 返回值是Option类型的,即可能是Some也可能是None
   * Some时返回值内容为Map[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
   * 表示:key是brokerId,value是该brokerId对应的TopicAndPartition-与该TopicAndPartition要写入的message集合映射关系
   */
  def partitionAndCollate(messages: Seq[KeyedMessage[K,Message]]): Option[Map[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]] = {
    //key是partition的leader所在节点,value是向该节点发送哪些topic-partition以及对应的信息
    val ret = new HashMap[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
    try {
      for (message <- messages) {//循环处理每一个信息元素
        val topicPartitionsList = getPartitionListForTopic(message)//获取该KeyedMessage要存储的topic对应的partition对象集合
        val partitionIndex = getPartition(message.topic, message.partitionKey, topicPartitionsList)//获取该key要存储到哪个partition中
        val brokerPartition = topicPartitionsList(partitionIndex)//获取最后分配的partiton对象

        // postpone the failure until the send operation, so that requests for other brokers are handled correctly 获取该partition对应的leader节点
        val leaderBrokerId = brokerPartition.leaderBrokerIdOpt.getOrElse(-1)

        //找到准备向该节点发送的topic-partition-message集合
        var dataPerBroker: HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]] = null
        ret.get(leaderBrokerId) match {
          case Some(element) =>
            dataPerBroker = element.asInstanceOf[HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
          case None =>
            dataPerBroker = new HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]]
            ret.put(leaderBrokerId, dataPerBroker)
        }

        val topicAndPartition = TopicAndPartition(message.topic, brokerPartition.partitionId)
        var dataPerTopicPartition: ArrayBuffer[KeyedMessage[K,Message]] = null //准备发送的信息集合,属于同一个topic-partition
        dataPerBroker.get(topicAndPartition) match {
          case Some(element) =>
            dataPerTopicPartition = element.asInstanceOf[ArrayBuffer[KeyedMessage[K,Message]]]
          case None =>
            dataPerTopicPartition = new ArrayBuffer[KeyedMessage[K,Message]]
            dataPerBroker.put(topicAndPartition, dataPerTopicPartition)
        }
        dataPerTopicPartition.append(message) //向集合中添加该信息
      }
      Some(ret)
    }catch {    // Swallow recoverable exceptions and return None so that they can be retried.
      case ute: UnknownTopicOrPartitionException => warn("Failed to collate messages by topic,partition due to: " + ute.getMessage); None
      case lnae: LeaderNotAvailableException => warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage); None
      case oe: Throwable => error("Failed to collate messages by topic, partition due to: " + oe.getMessage); None
    }
  }

  /**
   * 获取该KeyedMessage要存储的topic对应的partition对象集合,即该topic目前有多少个partiiton,以及每一个partition对应的leader节点
   */
  private def getPartitionListForTopic(m: KeyedMessage[K,Message]): Seq[PartitionAndLeader] = {
    /**
     * 获取该topic对应的元数据集合信息
     * 并且元数据集合信息是PartitionAndLeader集合形式返回,并且按照partitionId排序了的集合
     */
    val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic, correlationId.getAndIncrement)
    
    //打印日志,该topic在哪些个partition中
    debug("Broker partitions registered for topic: %s are %s"
      .format(m.topic, topicPartitionsList.map(p => p.partitionId).mkString(",")))
      
    val totalNumPartitions = topicPartitionsList.length
    if(totalNumPartitions == 0)
      throw new NoBrokersForPartitionException("Partition key = " + m.key)
    topicPartitionsList
  }

  /**
   * Retrieves the partition id and throws an UnknownTopicOrPartitionException if
   * the value of partition is not between 0 and numPartitions-1
   * @param topic The topic
   * @param key the partition key,按照该key去分配partition
   * @param topicPartitionList the list of available partitions 属于该topic的所有partition集合
   * @return the partition id,返回partition集合的index,将该message存储到该partition中
   * 获取该key要存储到哪个partition中
   */
  private def getPartition(topic: String, key: Any, topicPartitionList: Seq[PartitionAndLeader]): Int = {
    val numPartitions = topicPartitionList.size
    if(numPartitions <= 0)
      throw new UnknownTopicOrPartitionException("Topic " + topic + " doesn't exist")
    val partition =
      if(key == null) {
        // If the key is null, we don't really need a partitioner
        // So we look up in the send partition cache for the topic to decide the target partition
        val id = sendPartitionPerTopicCache.get(topic)
        id match {
          case Some(partitionId) =>
            // directly return the partitionId without checking availability of the leader,
            // since we want to postpone the failure until the send operation anyways
            partitionId
          case None =>
            val availablePartitions = topicPartitionList.filter(_.leaderBrokerIdOpt.isDefined)
            if (availablePartitions.isEmpty)
              throw new LeaderNotAvailableException("No leader for any partition in topic " + topic)
            val index = Utils.abs(Random.nextInt) % availablePartitions.size
            val partitionId = availablePartitions(index).partitionId
            sendPartitionPerTopicCache.put(topic, partitionId)
            partitionId
        }
      } else
        partitioner.partition(key, numPartitions)
    if(partition < 0 || partition >= numPartitions)
      throw new UnknownTopicOrPartitionException("Invalid partition id: " + partition + " for topic " + topic +
        "; Valid values are in the inclusive range of [0, " + (numPartitions-1) + "]")
    trace("Assigning message of topic %s and key %s to a selected partition %d".format(topic, if (key == null) "[none]" else key.toString, partition))
    partition
  }

  /**
   * Constructs and sends the produce request based on a map from (topic, partition) -> messages
   * 构造和发送生产者产生的数据,发送到brokerId节点上,发送的内容在map中,key是向该节点哪些topic-partation发送信息,value是发送的message集合
   * @param brokerId the broker that will receive the request 将要收到请求的节点Id,brokerId
   * @param messagesPerTopic the messages as a map from (topic, partition) -> messages 在该节点的每一个topic-partition上,对应发送哪些message集合
   * @return the set (topic, partitions) messages which incurred an error sending or processing,返回(topic, partitions)元组,表示哪些topic-partition发送过程中失败了
   *
   * 向该节点发送信息,返回没有发送成功的数据
   */
  private def send(brokerId: Int, messagesPerTopic: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) = {
    if(brokerId < 0) {//如果没有节点ID,则返回所有的topic-partition集合信息
      warn("Failed to send data since partitions %s don't have a leader".format(messagesPerTopic.map(_._1).mkString(",")))
      messagesPerTopic.keys.toSeq//说明所有的信息都发送失败
    } else if(messagesPerTopic.size > 0) {
      val currentCorrelationId = correlationId.getAndIncrement //发送的请求关联ID
      val producerRequest = new ProducerRequest(currentCorrelationId, config.clientId, config.requestRequiredAcks,
        config.requestTimeoutMs, messagesPerTopic)
      var failedTopicPartitions = Seq.empty[TopicAndPartition] //失败的topic-artation集合
      try {
        val syncProducer = producerPool.getProducer(brokerId) //获取该节点对应的请求连接
        debug("Producer sending messages with correlation id %d for topics %s to broker %d on %s:%d"
          .format(currentCorrelationId, messagesPerTopic.keySet.mkString(","), brokerId, syncProducer.config.host, syncProducer.config.port))
        val response = syncProducer.send(producerRequest) //发送请求,等待回复
        debug("Producer sent messages with correlation id %d for topics %s to broker %d on %s:%d"
          .format(currentCorrelationId, messagesPerTopic.keySet.mkString(","), brokerId, syncProducer.config.host, syncProducer.config.port))
        if(response != null) {//说明可能有失败的
          if (response.status.size != producerRequest.data.size)
            throw new KafkaException("Incomplete response (%s) for producer request (%s)".format(response, producerRequest))

          //打印发送成功的信息
          if (logger.isTraceEnabled) {
            val successfullySentData = response.status.filter(_._2.error == ErrorMapping.NoError)
            successfullySentData.foreach(m => messagesPerTopic(m._1).foreach(message =>
              trace("Successfully sent message: %s".format(if(message.message.isNull) null else Utils.readString(message.message.payload)))))
          }
          //过滤,获取发送失败的信息
          val failedPartitionsAndStatus = response.status.filter(_._2.error != ErrorMapping.NoError).toSeq
          failedTopicPartitions = failedPartitionsAndStatus.map(partitionStatus => partitionStatus._1)
          if(failedTopicPartitions.size > 0) {
            val errorString = failedPartitionsAndStatus
              .sortWith((p1, p2) => p1._1.topic.compareTo(p2._1.topic) < 0 ||
                                    (p1._1.topic.compareTo(p2._1.topic) == 0 && p1._1.partition < p2._1.partition))
              .map{
                case(topicAndPartition, status) =>
                  topicAndPartition.toString + ": " + ErrorMapping.exceptionFor(status.error).getClass.getName
              }.mkString(",")
            warn("Produce request with correlation id %d failed due to %s".format(currentCorrelationId, errorString))
          }
          failedTopicPartitions
        } else {//说明全部发送结束,没有失败的
          Seq.empty[TopicAndPartition]
        }
      } catch {
        case t: Throwable =>
          warn("Failed to send producer request with correlation id %d to broker %d with data for partitions %s"
            .format(currentCorrelationId, brokerId, messagesPerTopic.map(_._1).mkString(",")), t)
          messagesPerTopic.keys.toSeq //全部的topic-partition都发送失败
      }
    } else {
      List.empty //返回空集合
    }
  }

  /**
   * 参数是要发送的topic-partition与该partition等待发送的数据集合
   * 将每一个topic-partition的KeyedMessage集合转换成ByteBufferMessageSet对象,并且该对象可能会被压缩处理了,减少发送数据量
   */
  private def groupMessagesToSet(messagesPerTopicAndPartition: collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]) = {
    /** enforce the compressed.topics config here.可能会强制压缩信息
      *  If the compression codec is anything other than NoCompressionCodec,
      *    Enable compression only for specified topics if any
      *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
      *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
      */
    val messagesPerTopicPartition = messagesPerTopicAndPartition.map { case (topicAndPartition, messages) =>
      val rawMessages = messages.map(_.message)
      ( topicAndPartition,
        config.compressionCodec match {
          case NoCompressionCodec =>
            debug("Sending %d messages with no compression to %s".format(messages.size, topicAndPartition))
            new ByteBufferMessageSet(NoCompressionCodec, rawMessages: _*)
          case _ =>
            config.compressedTopics.size match {//判断是否是只压缩某些topic
              case 0 =>
                debug("Sending %d messages with compression codec %d to %s"
                  .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                //说明全部都要压缩
                new ByteBufferMessageSet(config.compressionCodec, rawMessages: _*)
              case _ =>
                if(config.compressedTopics.contains(topicAndPartition.topic)) {//仅仅包含指定要压缩的topic的数据才会被压缩
                  debug("Sending %d messages with compression codec %d to %s"
                    .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                  new ByteBufferMessageSet(config.compressionCodec, rawMessages: _*)
                }
                else {//不参与压缩
                  debug("Sending %d messages to %s with no compression as it is not in compressed.topics - %s"
                    .format(messages.size, topicAndPartition, config.compressedTopics.toString))
                  new ByteBufferMessageSet(NoCompressionCodec, rawMessages: _*)
                }
            }
        }
        )
    }
    messagesPerTopicPartition
  }

  def close() {
    if (producerPool != null)
      producerPool.close
  }
}
