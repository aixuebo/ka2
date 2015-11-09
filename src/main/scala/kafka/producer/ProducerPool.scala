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

import kafka.cluster.Broker
import java.util.Properties
import collection.mutable.HashMap
import java.lang.Object
import kafka.utils.Logging
import kafka.api.TopicMetadata
import kafka.common.UnavailableProducerException

/**
 * 一个生产者客户端对应一个该生产者连接池,保证该生产者客户端连接不同的Broker,每一个Broker保持一个连接即可
 */
object ProducerPool {
  /**
   * Used in ProducerPool to initiate a SyncProducer connection with a broker.
   * 根据配置信息创建一个同步的生产者,该生产者连接Broker这个节点
   */
  def createSyncProducer(config: ProducerConfig, broker: Broker): SyncProducer = {
    val props = new Properties()
    props.put("host", broker.host)//设置该生产者需要连接哪台Broker节点
    props.put("port", broker.port.toString)//设置该生产者需要连接哪台Broker节点
    props.putAll(config.props.props)
    new SyncProducer(new SyncProducerConfig(props))
  }
}

class ProducerPool(val config: ProducerConfig) extends Logging {
  //记录该生产者连接每一台Broker节点的对应关系
  //可以保证该生产者连接每一个Broker使用一个连接即可
  //key是该生产者要连接的Broker的Id,value是该生产者产生的同步生产者对象SyncProducer
  private val syncProducers = new HashMap[Int, SyncProducer]
  private val lock = new Object()

  //该生产者创建连接这些topic的leader节点的同步连接
  def updateProducer(topicMetadata: Seq[TopicMetadata]) {
    val newBrokers = new collection.mutable.HashSet[Broker]
    topicMetadata.foreach(tmd => {
      tmd.partitionsMetadata.foreach(pmd => {
        if(pmd.leader.isDefined)
          newBrokers+=(pmd.leader.get)
      })
    })
    lock synchronized {
      newBrokers.foreach(b => {
        if(syncProducers.contains(b.id)){
          syncProducers(b.id).close()
          syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b))
        } else
          syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b))
      })
    }
  }

  //获取该生产者连接ID为brokerId的节点Broker对应的同步生产者SyncProducer,没有则抛异常,说明该同步生产者还没有创建
  def getProducer(brokerId: Int) : SyncProducer = {
    lock.synchronized {
      val producer = syncProducers.get(brokerId)
      producer match {
        case Some(p) => p
        case None => throw new UnavailableProducerException("Sync producer for broker id %d does not exist".format(brokerId))
      }
    }
  }

  /**
   * Closes all the producers in the pool
   * 关闭该生产者连接的所有Broker节点
   */
  def close() = {
    lock.synchronized {
      info("Closing all sync producers")
      val iter = syncProducers.values.iterator
      while(iter.hasNext)
        iter.next.close
    }
  }
}
