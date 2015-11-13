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

import scala.collection._
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{Json, ZKGroupDirs, ZkUtils, Logging, Utils}
import kafka.common.KafkaException

private[kafka] trait TopicCount {

  def getConsumerThreadIdsPerTopic: Map[String, Set[ConsumerThreadId]] //key是topic,value是topic多少个线程去消费,每一个消费线程持有一个ConsumerThreadId对象
  def getTopicCountMap: Map[String, Int] //key是topic,value是topic有多少个线程去消费
  def pattern: String//white_list、black_list、static之一

}

//可以支持按照消费者名称排序，相同消费者的时候以线程id排序
case class ConsumerThreadId(consumer: String, threadId: Int) extends Ordered[ConsumerThreadId] {
  override def toString = "%s-%d".format(consumer, threadId)

  def compare(that: ConsumerThreadId) = toString.compare(that.toString)
}

private[kafka] object TopicCount extends Logging {
  val whiteListPattern = "white_list"
  val blackListPattern = "black_list"
  val staticPattern = "static"

  def makeThreadId(consumerIdString: String, threadId: Int) = consumerIdString + "-" + threadId

  //返回HashMap[String, Set[ConsumerThreadId]] key是topic,value是该topic上多个线程ID可以读取topic信息
  def makeConsumerThreadIdsPerTopic(consumerIdString: String,
                                    topicCountMap: Map[String,  Int]) = {
    val consumerThreadIdsPerTopicMap = new mutable.HashMap[String, Set[ConsumerThreadId]]()
    for ((topic, nConsumers) <- topicCountMap) {
      val consumerSet = new mutable.HashSet[ConsumerThreadId]
      assert(nConsumers >= 1)
      for (i <- 0 until nConsumers)
        consumerSet += ConsumerThreadId(consumerIdString, i)
      consumerThreadIdsPerTopicMap.put(topic, consumerSet)
    }
    consumerThreadIdsPerTopicMap
  }

  ///consumers/${group}/ids/${consumerId} 内容{"pattern":"white_list、black_list、static之一","subscription":{"${topic}":2,"${topic}":2}  }
  def constructTopicCount(group: String, consumerId: String, zkClient: ZkClient, excludeInternalTopics: Boolean) : TopicCount = {
    val dirs = new ZKGroupDirs(group)
    val topicCountString = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId)._1
    var subscriptionPattern: String = null //white_list、black_list、static之一
    var topMap: Map[String, Int] = null // {"${topic}":2,"${topic}":2} key是topic,value是该topic在group中要有多少个线程去读取
    try {
      Json.parseFull(topicCountString) match {
        
        case Some(m) =>
          val consumerRegistrationMap = m.asInstanceOf[Map[String, Any]]
          consumerRegistrationMap.get("pattern") match {
            case Some(pattern) => subscriptionPattern = pattern.asInstanceOf[String]
            case None => throw new KafkaException("error constructing TopicCount : " + topicCountString)
          }
          consumerRegistrationMap.get("subscription") match {
            case Some(sub) => topMap = sub.asInstanceOf[Map[String, Int]]
            case None => throw new KafkaException("error constructing TopicCount : " + topicCountString)
          }
        case None => throw new KafkaException("error constructing TopicCount : " + topicCountString)
      }
    } catch {
      case e: Throwable =>
        error("error parsing consumer json string " + topicCountString, e)
        throw e
    }

    val hasWhiteList = whiteListPattern.equals(subscriptionPattern)
    val hasBlackList = blackListPattern.equals(subscriptionPattern)

    if (topMap.isEmpty || !(hasWhiteList || hasBlackList)) {//表示topMap是空,或者(没有设置黑名单,并且也没有设置白名单)
      new StaticTopicCount(consumerId, topMap)
    } else {
      val regex = topMap.head._1
      val numStreams = topMap.head._2
      val filter =
        if (hasWhiteList)
          new Whitelist(regex)
        else
          new Blacklist(regex)
      new WildcardTopicCount(zkClient, consumerId, filter, numStreams, excludeInternalTopics)
    }
  }

  def constructTopicCount(consumerIdString: String, topicCount: Map[String, Int]) =
    new StaticTopicCount(consumerIdString, topicCount)

  def constructTopicCount(consumerIdString: String, filter: TopicFilter, numStreams: Int, zkClient: ZkClient, excludeInternalTopics: Boolean) =
    new WildcardTopicCount(zkClient, consumerIdString, filter, numStreams, excludeInternalTopics)

}

//key是消费者ID,value是该消费者消费多少个topic,Map[String, Int] key是topic,value是该topic多少个线程消费
private[kafka] class StaticTopicCount(val consumerIdString: String,
                                val topicCountMap: Map[String, Int])
                                extends TopicCount {

    //返回HashMap[String, Set[ConsumerThreadId]] key是topic,value是该topic上多个线程ID可以读取topic信息
  def getConsumerThreadIdsPerTopic = TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap)

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case n: StaticTopicCount => consumerIdString == n.consumerIdString && topicCountMap == n.topicCountMap
      case _ => false
    }
  }

  def getTopicCountMap = topicCountMap

  def pattern = TopicCount.staticPattern
}

/**
 * @topicFilter 正则表达式过滤器
 * @excludeInternalTopics true表示要确保topic不能是kafka内部topic名称,例如__consumer_offsets
 * @numStreams 每一个topic要产生多少个线程读取
 */
private[kafka] class WildcardTopicCount(zkClient: ZkClient,
                                        consumerIdString: String,
                                        topicFilter: TopicFilter,
                                        numStreams: Int,
                                        excludeInternalTopics: Boolean) extends TopicCount {
  def getConsumerThreadIdsPerTopic = {
    /**
     * 1.获取线上所有的topic集合,即读取/brokers/topics
     * 2.过滤topic,仅仅需要找到符合正则表达式的topic即可
     */
    val wildcardTopics = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath)
                         .filter(topic => topicFilter.isTopicAllowed(topic, excludeInternalTopics))
                         
    //根据过滤后符合的topic,生成  [String, Set[ConsumerThreadId]]信息,key是topic,value是该topic多少个线程消费  
    TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString, Map(wildcardTopics.map((_, numStreams)): _*))
  }

  //返回key是正则表达式,value是每一个topic要产生多少个线程读取,该Map的size=1
  def getTopicCountMap = Map(Utils.JSONEscapeString(topicFilter.regex) -> numStreams)

  def pattern: String = {
    topicFilter match {
      case wl: Whitelist => TopicCount.whiteListPattern
      case bl: Blacklist => TopicCount.blackListPattern
    }
  }

}

