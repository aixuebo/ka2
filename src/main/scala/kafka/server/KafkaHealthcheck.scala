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

import kafka.utils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import java.net.InetAddress


/**
 * This class registers the broker in zookeeper to allow 
 * other brokers and consumers to detect failures. It uses an ephemeral znode with the path:
 *   /brokers/[0...N] --> advertisedHost:advertisedPort
 * 该类会注册broker到zookeeper下,去允许其他broker和消费者去察觉该broker是否失败,
 * 他使用的是ephemeral节点,路径是/brokers/[0...N],内容是advertisedHost:advertisedPort
 * 
 * Right now our definition of health is fairly naive. If we register in zk we are healthy, otherwise
 * we are dead.
 * 现在我们定义是否健康的标准是非常的简单的,即如果zookeeper下有该broker,则说明该节点健康,不存在则认为该节点已经死了
 * 
 * 将broker注册到zookeeper的/brokers/ids/${brokerId}下
 * 
 * 一个broker对应一个该类
 */
class KafkaHealthcheck(private val brokerId: Int, //要注册的节点ID
                       private val advertisedHost: String, //节点所在host
                       private val advertisedPort: Int,//节点所在端口
                       private val zkSessionTimeoutMs: Int,//连接zookeeper的超时时间
                       private val zkClient: ZkClient) extends Logging {//zookeeper客户端

  val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + brokerId // /brokers/ids/${brokerId}
  
  val sessionExpireListener = new SessionExpireListener

  //建立监听对象,并且向zookeeper注册
  def startup() {
    zkClient.subscribeStateChanges(sessionExpireListener)
    register()
  }

  /**
   * Register this broker as "alive" in zookeeper
   * 将该broker注册到 zookeeper中
   */
  def register() {
    val advertisedHostName = 
      if(advertisedHost == null || advertisedHost.trim.isEmpty) 
        InetAddress.getLocalHost.getCanonicalHostName 
      else
        advertisedHost
    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    ZkUtils.registerBrokerInZk(zkClient, brokerId, advertisedHostName, advertisedPort, zkSessionTimeoutMs, jmxPort)
  }

  /**
   *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
   *  connection for us. We need to re-register this broker in the broker registry.
   *  session过期时,我们需要重新与zookeeper建立连接
   */
  class SessionExpireListener() extends IZkStateListener {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      info("re-registering broker info in ZK for broker " + brokerId)
      register()
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    }
  }

}
