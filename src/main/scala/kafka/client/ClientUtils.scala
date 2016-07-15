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
 package kafka.client

import scala.collection._
import kafka.cluster._
import kafka.api._
import kafka.producer._
import kafka.common.{ErrorMapping, KafkaException}
import kafka.utils.{Utils, Logging}
import java.util.Properties
import util.Random
 import kafka.network.BlockingChannel
 import kafka.utils.ZkUtils._
 import org.I0Itec.zkclient.ZkClient
 import java.io.IOException
import org.apache.kafka.common.utils.Utils.{getHost, getPort}

 /**
 * Helper functions common to clients (producer, consumer, or admin)
  * 客户端调用的工具包
 */
object ClientUtils extends Logging{

  /**
   * Used by the producer to send a metadata request since it has access to the ProducerConfig
   * @param topics The topics for which the metadata needs to be fetched
   * @param brokers The brokers in the cluster as configured on the producer through metadata.broker.list
   * @param producerConfig The producer's config
   * @return topic metadata response
   * 从Broker节点集合中为topic集合中每一个topic抓取元数据
   */
  def fetchTopicMetadata(topics: Set[String], brokers: Seq[Broker], producerConfig: ProducerConfig, correlationId: Int): TopicMetadataResponse = {
    var fetchMetaDataSucceeded: Boolean = false//抓取结果是否成功
    var i: Int = 0
    val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationId, producerConfig.clientId, topics.toSeq)//抓取请求
    var topicMetadataResponse: TopicMetadataResponse = null//抓取请求返回值
    var t: Throwable = null
    // shuffle the list of brokers before sending metadata requests so that most requests don't get routed to the
    // same broker 打乱brokers集合的顺序,目的是所有请求不会都路由到相同的broker节点上
    val shuffledBrokers = Random.shuffle(brokers)
    
    while(i < shuffledBrokers.size && !fetchMetaDataSucceeded) {//如果节点还存在,并且没有抓取成功,则就进行while循环抓取
      val producer: SyncProducer = ProducerPool.createSyncProducer(producerConfig, shuffledBrokers(i))
      info("Fetching metadata from broker %s with correlation id %d for %d topic(s) %s".format(shuffledBrokers(i), correlationId, topics.size, topics))
      try {
        topicMetadataResponse = producer.send(topicMetadataRequest)
        fetchMetaDataSucceeded = true
      }
      catch {
        case e: Throwable =>
          warn("Fetching topic metadata with correlation id %d for topics [%s] from broker [%s] failed"
            .format(correlationId, topics, shuffledBrokers(i).toString), e)
          t = e
      } finally {
        i = i + 1
        producer.close()
      }
    }
    
    if(!fetchMetaDataSucceeded) {//如果抓取失败,抛异常
      throw new KafkaException("fetching topic metadata for topics [%s] from broker [%s] failed".format(topics, shuffledBrokers), t)
    } else {//说明成功抓取多少个topic，以及哪些topic被抓取了元数据
      debug("Successfully fetched metadata for %d topic(s) %s".format(topics.size, topics))
    }
    return topicMetadataResponse
  }

  /**
   * Used by a non-producer client to send a metadata request
   * @param topics The topics for which the metadata needs to be fetched
   * @param brokers The brokers in the cluster as configured on the client
   * @param clientId The client's identifier
   * @return topic metadata response
   */
  def fetchTopicMetadata(topics: Set[String], brokers: Seq[Broker], clientId: String, timeoutMs: Int,
                         correlationId: Int = 0): TopicMetadataResponse = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers.map(_.connectionString).mkString(","))//改成host:port,host:port
    props.put("client.id", clientId)
    props.put("request.timeout.ms", timeoutMs.toString)
    val producerConfig = new ProducerConfig(props)
    fetchTopicMetadata(topics, brokers, producerConfig, correlationId)
  }

  /**
   * Parse a list of broker urls in the form host1:port1, host2:port2, ... 
   * 解析host1:port1, host2:port2为Broker集合
   */
  def parseBrokerList(brokerListStr: String): Seq[Broker] = {
    val brokersStr = Utils.parseCsvList(brokerListStr)//按照空格拆分成集合,过滤掉空的元素

    /**
demo:
val a = List(100,200,300)
a indices // (0,1,2)
a zipWithIndex // ((100,0), (200,1), (300,2))
     */
    brokersStr.zipWithIndex.map { case (address, brokerId) =>
      new Broker(brokerId, getHost(address), getPort(address))
    }
  }

   /**
    * Creates a blocking channel to a random broker
    * 连接集群中随机选择任意一个broker节点,返回连接的socket
    */
   def channelToAnyBroker(zkClient: ZkClient, socketTimeoutMs: Int = 3000) : BlockingChannel = {
     var channel: BlockingChannel = null
     var connected = false
     while (!connected) {
       //获取当前集群中合法的broker的对象集合.并且已经排序后返回
       val allBrokers = getAllBrokersInCluster(zkClient)
       Random.shuffle(allBrokers).find { broker =>
         trace("Connecting to broker %s:%d.".format(broker.host, broker.port))
         try {
           channel = new BlockingChannel(broker.host, broker.port, BlockingChannel.UseDefaultBufferSize, BlockingChannel.UseDefaultBufferSize, socketTimeoutMs)
           channel.connect()
           debug("Created channel to broker %s:%d.".format(channel.host, channel.port))
           true
         } catch {
           case e: Exception =>
             if (channel != null) channel.disconnect()
             channel = null
             info("Error while creating channel to %s:%d.".format(broker.host, broker.port))
             false
         }
       }
       connected = if (channel == null) false else true
     }

     channel
   }

   /**
    * Creates a blocking channel to the offset manager of the given group
    * 参数retryBackOffMs 表示每次尝试时,要休息多少毫秒
    * 
    * 先请求任意一台broker,然后获取该group所要访问的真正的数据broker,然后与真正的broker再建立连接
    */
   def channelToOffsetManager(group: String, zkClient: ZkClient, socketTimeoutMs: Int = 3000, retryBackOffMs: Int = 1000) = {
     //连接集群中随机选择任意一个broker节点,返回连接的socket
     var queryChannel = channelToAnyBroker(zkClient)//返回值是BlockingChannel类型的

     var offsetManagerChannelOpt: Option[BlockingChannel] = None

     while (!offsetManagerChannelOpt.isDefined) {

       var coordinatorOpt: Option[Broker] = None//消费者获取该group服务器被消费的元数据所在主节点

       while (!coordinatorOpt.isDefined) {
         try {
           //如果连接已断,在任意选取集群一个broker节点,创建socket连接
           if (!queryChannel.isConnected)
             queryChannel = channelToAnyBroker(zkClient)
             //此时已经连接到哪个节点
           debug("Querying %s:%d to locate offset manager for %s.".format(queryChannel.host, queryChannel.port, group))
           
           queryChannel.send(ConsumerMetadataRequest(group))
           val response = queryChannel.receive()
           val consumerMetadataResponse =  ConsumerMetadataResponse.readFrom(response.buffer)
           debug("Consumer metadata response: " + consumerMetadataResponse.toString)
           if (consumerMetadataResponse.errorCode == ErrorMapping.NoError)
             coordinatorOpt = consumerMetadataResponse.coordinatorOpt
           else {
             debug("Query to %s:%d to locate offset manager for %s failed - will retry in %d milliseconds."
                  .format(queryChannel.host, queryChannel.port, group, retryBackOffMs))
             Thread.sleep(retryBackOffMs)
           }
         }
         catch {
           case ioe: IOException =>
             info("Failed to fetch consumer metadata from %s:%d.".format(queryChannel.host, queryChannel.port))
             queryChannel.disconnect()
         }
       }

       val coordinator = coordinatorOpt.get
       if (coordinator.host == queryChannel.host && coordinator.port == queryChannel.port) {
         offsetManagerChannelOpt = Some(queryChannel)
       } else {
         val connectString = "%s:%d".format(coordinator.host, coordinator.port)
         var offsetManagerChannel: BlockingChannel = null
         try {
           debug("Connecting to offset manager %s.".format(connectString))
           offsetManagerChannel = new BlockingChannel(coordinator.host, coordinator.port,
                                                      BlockingChannel.UseDefaultBufferSize,
                                                      BlockingChannel.UseDefaultBufferSize,
                                                      socketTimeoutMs)
           offsetManagerChannel.connect()
           offsetManagerChannelOpt = Some(offsetManagerChannel)
           queryChannel.disconnect()//关闭第一个连接器
         }
         catch {
           case ioe: IOException => // offsets manager may have moved
             info("Error while connecting to %s.".format(connectString))
             if (offsetManagerChannel != null) offsetManagerChannel.disconnect()
             Thread.sleep(retryBackOffMs)
             offsetManagerChannelOpt = None // just in case someone decides to change shutdownChannel to not swallow exceptions
         }
       }
     }

     offsetManagerChannelOpt.get
   }
 }
