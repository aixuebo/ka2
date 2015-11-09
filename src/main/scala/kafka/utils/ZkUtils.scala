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

package kafka.utils

import kafka.cluster.{Broker, Cluster}
import kafka.consumer.{ConsumerThreadId, TopicCount}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException,
  ZkMarshallingError, ZkBadVersionException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import collection._
import kafka.api.LeaderAndIsr
import org.apache.zookeeper.data.Stat
import kafka.admin._
import kafka.common.{KafkaException, NoEpochForPartitionException}
import kafka.controller.ReassignedPartitionsContext
import kafka.controller.KafkaController
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.common.TopicAndPartition
import scala.collection
import scala.Some

object ZkUtils extends Logging {
  
  /**
    * /consumers
    * /consumers/${group}
    * /consumers/${group}/ids
    * /consumers/${group}/offsets/${topic}
    * /consumers/${owners}/offsets/${topic}
    * 1./consumers/${group}/ids/${consumerId} 内容{"pattern":"white_list、black_list、static之一","subscription":{"${topic}":2,"${topic}":2}  } 表示该消费者组内的某一个消费者可以消费哪些topic,以及每一个topic消费多少个partition
    * /consumers/[groupId]/ids/[consumerIdString],其中consumerIdString生成规则即表示此consumer目前所消费的topic + partitions列表.
    * 2./consumers/[groupId]/offsets/[topic]/[partitionId] 存储 long (offset)
                      用来跟踪每个consumer目前所消费的partition中最大的offset
                      此znode为持久节点,可以看出offset跟group_id有关,以表明当消费者组(consumer group)中一个消费者失效,
                      重新触发balance,其他consumer可以继续消费.
     *3./consumers/${group}/ids/消费者自定义ID名称,该意义是可以知道该group下面的消费者多少个,如果挂了一个消费者,或者挂了一个broker,可以动态调节消费者消费哪些topic-partition,内容如下:
       {
      “version”:1,
      “subscription”:{“test_kafka”:3},//订阅topic列表
      “topic名称”: consumer中topic消费者线程数[与队列的分区数量有关]
      “pattern”:”static”,
      “timestamp”:”1416810012297″
      } 
     * 4.consumers/[groupId]/owners/[topic]/[partitionId]/consumer_thread
                          用来保存每个topic的的partition的是由那个消费者线程进行消费的信息。
   */
  val ConsumersPath = "/consumers"
  
  //集群的所有broker的ID集合作为他的子节点,例如 /brokers/ids/1 /brokers/ids/2 表示有2个broker节点,每一个节点的内容是 Json.encode(Map("version" -> 1, "host" -> host, "port" -> port, "jmx_port" -> jmxPort, "timestamp" -> timestamp))
    //ReplicaStateMachine类注册了该监听事件
  val BrokerIdsPath = "/brokers/ids"
    /**
1./brokers/topics/${topic}/partitions/${partitionId}/state 内容{leader:int,leader_epoch:int,isr:List[int],controller_epoch:int}
  存储该topic-partition的状态信息,即对应的实体是LeaderIsrAndControllerEpoch对象
"controller_epoch": 表示kafka集群中的中央控制器选举次数,
"leader": 表示该partition选举leader的brokerId,
"version": 版本编号默认为1,
"leader_epoch": 该partition leader选举次数,
"isr": [同步副本组brokerId列表]
  
2.从路径可以获取所有的topic-partition集合
3.从路径可以获取所有的topic集合
4./brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId备份存储
5.通过4可以获取topic-partition,该组合都在哪些节点有备份
6.PartitionStateMachine类注册了该监听事件 
7./brokers/topics/${topic}节点上 参见PartitionStateMachine类注册了该监听事件 
*/
  val BrokerTopicsPath = "/brokers/topics"
  
  
  ///config/topics/${topic}节点的内容是该topic的配置信息
  //内容格式:{version:1,config:{key=value,key=value}}
  val TopicConfigPath = "/config/topics"
  
  /**
   * /config/changes的子节点命名规则:
   * 参数name是以config_change_开头 + 数字,返回该数字
   * eg:config_change_5
   * 并且config_change_5节点的内容是改节点对应的topic名称,通过该名称,查询属性TopicConfigPath,从而获取/config/topics/${topic}节点的内容是该topic的配置信息
   * 
   * 该/config/changes节点属于临时配置一些更改内容的topic子节点,超过一定时间后,该子节点会被删除掉
   */
  val TopicConfigChangesPath = "/config/changes"//参见TopicConfigManager类
    
  val ControllerPath = "/controller"//是json格式,解析的内容是哪个broker节点是kafka主节点,存储内容:{brokerid:5}
    /**
controller_epoch节点的值是一个数字,kafka集群中第一个broker第一次启动时为1，以后只要集群中center controller中央控制器所在broker变更或挂掉，就会重新选举新的center controller，每次center controller变更controller_epoch值就会 + 1; 
     */
  val ControllerEpochPath = "/controller_epoch"
  
/**
/admin/reassign_partitions内容
{
  "version": 1,
  "partitions":
     [
        {
            "topic": "Foo",
            "partition": 1,
            "replicas": [0, 1, 3]
        }
     ]            
}
 */
  val ReassignPartitionsPath = "/admin/reassign_partitions"
/**
/admin/delete_topics内容 
{
  "version": 1,
  "topics": ["foo", "bar"]
}
*/
  //参见PartitionStateMachine类注册了该监听事件 
  val DeleteTopicsPath = "/admin/delete_topics"
  
/**
/admin/preferred_replica_election内容
{
  "version": 1,
  "partitions":
     [
        {
            "topic": "Foo",
            "partition": 1         
        },
        {
            "topic": "Bar",
            "partition": 0         
        }
     ]            
}
 */
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"

  //return /brokers/topics/${topic}
  def getTopicPath(topic: String): String = {
    BrokerTopicsPath + "/" + topic
  }
  //return /brokers/topics/${topic}/partitions
  def getTopicPartitionsPath(topic: String): String = {
    getTopicPath(topic) + "/partitions"
  }

  // /config/topics/${topic}
  def getTopicConfigPath(topic: String): String =
    TopicConfigPath + "/" + topic

  // /admin/delete_topics/${topic}
  def getDeleteTopicPath(topic: String): String =
    DeleteTopicsPath + "/" + topic

    
    //读取/controller节点的内容
  def getController(zkClient: ZkClient): Int = {
    readDataMaybeNull(zkClient, ControllerPath)._1 match {
      
      case Some(controller) => KafkaController.parseControllerId(controller)
      case None => throw new KafkaException("Controller doesn't exist")
    }
  }

  //return /brokers/topics/${topic}/partitions/${partitionId}
  def getTopicPartitionPath(topic: String, partitionId: Int): String =
    getTopicPartitionsPath(topic) + "/" + partitionId

  //return /brokers/topics/${topic}/partitions/${partitionId}/state
  def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String =
    getTopicPartitionPath(topic, partitionId) + "/" + "state"

//获取/brokers/ids节点的子节点集合,即获取当前集群中broker的ID集合,并且排序后返回
  def getSortedBrokerList(zkClient: ZkClient): Seq[Int] =
    ZkUtils.getChildren(zkClient, BrokerIdsPath).map(_.toInt).sorted

  //获取当前集群中合法的broker的对象集合.并且已经排序后返回
  def getAllBrokersInCluster(zkClient: ZkClient): Seq[Broker] = {
    val brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath).sorted
    //将ID转换成int,
    //然后每一个ID---读取/brokers/ids/${brokerId}的内容.即该brokerId对应的host和part最后组装成Broker对象集合返回
    //过滤掉Broker=none的对象
    //返回合法的Broker集合
    brokerIds.map(_.toInt).map(getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
  }

  // 获取/brokers/topics/ ${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
  def getLeaderAndIsrForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderAndIsr] = {
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition).map(_.leaderAndIsr)
  }

  // 初始化,建立持久化的path
  def setupCommonPaths(zkClient: ZkClient) {
    for(path <- Seq(ConsumersPath, BrokerIdsPath, BrokerTopicsPath, TopicConfigChangesPath, TopicConfigPath, DeleteTopicsPath))
      makeSurePersistentPathExists(zkClient, path)
  }

  /**
   * 读取/brokers/topics/${topic}/partitions/${partitionId}/state的内容,获取leader对应的信息
   */
  def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case Some(m) =>
            Some(m.asInstanceOf[Map[String, Any]].get("leader").get.asInstanceOf[Int])
          case None => None
        }
      case None => None
    }
  }

  /**
   * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
   * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
   * other broker will retry becoming leader with the same new epoch value.
   * 读取 /brokers/topics/${topic}/partitions/${partitionId}/state节点信息,返回该leader_epoch对应的内容
   */
  def getEpochForPartition(zkClient: ZkClient, topic: String, partition: Int): Int = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case None => throw new NoEpochForPartitionException("No epoch, leaderAndISR data for partition [%s,%d] is invalid".format(topic, partition))
          case Some(m) => m.asInstanceOf[Map[String, Any]].get("leader_epoch").get.asInstanceOf[Int]
        }
      case None => throw new NoEpochForPartitionException("No epoch, ISR path for partition [%s,%d] is empty"
        .format(topic, partition))
    }
  }

  /**
   * Gets the in-sync replicas (ISR) for a specific topic and partition
   * 读取/brokers/topics/${topic}/partitions/${partitionId}/state的内容,获取isr对应的值
   */
  def getInSyncReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case Some(m) => m.asInstanceOf[Map[String, Any]].get("isr").get.asInstanceOf[Seq[Int]]
          case None => Seq.empty[Int]
        }
      case None => Seq.empty[Int]
    }
  }

  /**
   * Gets the assigned replicas (AR) for a specific topic and partition
   * 1.读取/brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
   * 2.解析内容,属于该partition对应的备份节点集合
   */
  def getReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        Json.parseFull(jsonPartitionMap) match {
          case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
            case Some(replicaMap) => replicaMap.asInstanceOf[Map[String, Seq[Int]]].get(partition.toString) match {
              case Some(seq) => seq
              case None => Seq.empty[Int]
            }
            case None => Seq.empty[Int]
          }
          case None => Seq.empty[Int]
        }
      case None => Seq.empty[Int]
    }
  }

  //创建/brokers/ids/id节点,并且设置该节点的内容,如果冲突,即以前设置过内容,则进行更新操作
  def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, port: Int, timeout: Int, jmxPort: Int) {
    val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id
    val timestamp = SystemTime.milliseconds.toString
    val brokerInfo = Json.encode(Map("version" -> 1, "host" -> host, "port" -> port, "jmx_port" -> jmxPort, "timestamp" -> timestamp))
    val expectedBroker = new Broker(id, host, port)

    try {
      createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerInfo, expectedBroker,
        (brokerString: String, broker: Any) => Broker.createBroker(broker.asInstanceOf[Broker].id, brokerString).equals(broker.asInstanceOf[Broker]),
        timeout)

    } catch {
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
          + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
          + "else you have shutdown this broker and restarted it faster than the zookeeper "
          + "timeout so it appears to be re-registering.")
    }
    info("Registered broker %d at path %s with address %s:%d.".format(id, brokerIdPath, host, port))
  }

  def getConsumerPartitionOwnerPath(group: String, topic: String, partition: Int): String = {
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    topicDirs.consumerOwnerDir + "/" + partition
  }

//更新/brokers/topics/${topic}/partitions/${partitionId}/state下的json内容信息,该方法仅仅是生成待写入的json字符串
  def leaderAndIsrZkData(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int): String = {
    Json.encode(Map("version" -> 1, "leader" -> leaderAndIsr.leader, "leader_epoch" -> leaderAndIsr.leaderEpoch,
                    "controller_epoch" -> controllerEpoch, "isr" -> leaderAndIsr.isr))
  }

  /**
   * Get JSON partition to replica map from zookeeper.
   * 将Map转换成json字符串,该Map是/brokers/topics/${topic}的内容
   * 
   * @param    Map[String, Seq[Int]] key是partition,value是该partition所在的备份broker的ID集合,例如
   * 例如:
   * /brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId备份存储
   */
  def replicaAssignmentZkData(map: Map[String, Seq[Int]]): String = {
    Json.encode(Map("version" -> 1, "partitions" -> map))
  }

  /**
   *  make sure a persistent path exists in ZK. Create the path if not exist.
   *  如果path不存在则创建一个持久化的path,总之该方法确保path持久化存在
   */
  def makeSurePersistentPathExists(client: ZkClient, path: String) {
    if (!client.exists(path))
      client.createPersistent(path, true) // won't throw NoNodeException or NodeExistsException
  }

  /**
   *  create the parent path
   *  持久化的创建path的父路径
   */
  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  /**
   * Create an ephemeral node with the given path and data. Create parents if necessary.
   * 创建临时的path,并且存储数据,该数据path是递归可以一直产生下去的
   */
  private def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistException if node already exists.
   * 创建临时节点path,并且存储数据data
   * 注意:如果path已经存在,则要判断data与存在的data是否相同,如果不同则抛异常
   */
  def createEphemeralPathExpectConflict(client: ZkClient, path: String, data: String): Unit = {
    try {
      createEphemeralPath(client, path, data)
    } catch {
      case e: ZkNodeExistsException => {
        // this can happen when there is connection loss; make sure the data is what we intend to write
        var storedData: String = null
        try {
          storedData = readData(client, path)._1
        } catch {
          case e1: ZkNoNodeException => // the node disappeared; treat as if node existed and let caller handles this
          case e2: Throwable => throw e2
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData)
          throw e
        } else {
          // otherwise, the creation succeeded, return normally
          info(path + " exists with value " + data + " during connection loss; this is ok")
        }
      }
      case e2: Throwable => throw e2
    }
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistsException if node already exists.
   * Handles the following ZK session timeout bug:
   *
   * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
   *
   * Upon receiving a NodeExistsException, read the data from the conflicted path and
   * trigger the checker function comparing the read data and the expected data,
   * If the checker function returns true then the above bug might be encountered, back off and retry;
   * otherwise re-throw the exception
   * 
   * 创建临时节点path,并且存储数据data
   * 注意:如果path已经存在,则要判断data与存在的data是否相同,如果不同则抛异常
   */
  def createEphemeralPathExpectConflictHandleZKBug(zkClient: ZkClient, path: String, data: String, expectedCallerData: Any, checker: (String, Any) => Boolean, backoffTime: Int): Unit = {
    while (true) {
      try {
        createEphemeralPathExpectConflict(zkClient, path, data)
        return
      } catch {
        case e: ZkNodeExistsException => {
          // An ephemeral node may still exist even after its corresponding session has expired
          // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted
          // and hence the write succeeds without ZkNodeExistsException
          ZkUtils.readDataMaybeNull(zkClient, path)._1 match {
            case Some(writtenData) => {//数据节点上的内容
              if (checker(writtenData, expectedCallerData)) {
                info("I wrote this conflicted ephemeral node [%s] at %s a while back in a different session, ".format(data, path)
                  + "hence I will backoff for this node to be deleted by Zookeeper and retry")

                Thread.sleep(backoffTime)
              } else {
                throw e
              }
            }
            case None => // the node disappeared; retry creating the ephemeral node immediately
          }
        }
        case e2: Throwable => throw e2
      }
    }
  }

  /**
   * Create an persistent node with the given path and data. Create parents if necessary.
   * 创建永久的path,并且写入data数据,
   * 注意:该方法会递归path的父节点不断被创建
   */
  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      //如果节点不存在,则不断创建父节点
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

  //创建永久的path,并且写入data数据,
  def createSequentialPersistentPath(client: ZkClient, path: String, data: String = ""): String = {
    client.createPersistentSequential(path, data)
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parrent directory if necessary. Never throw NodeExistException.
   * Return the updated path zkVersion
   * 更新path下的数据,如果path不存在则创建该path,在写入数据
   */
  def updatePersistentPath(client: ZkClient, path: String, data: String) = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            client.writeData(path, data)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
   * exist, the current version is not the expected version, etc.) return (false, -1)
   *
   * When there is a ConnectionLossException during the conditional update, zkClient will retry the update and may fail
   * since the previous update may have succeeded (but the stored zkVersion no longer matches the expected one).
   * In this case, we will run the optionalChecker to further check if the previous write did indeed succeeded.
   * 
   *   返回元组,1表示是否写入成功.2表示升级后的版本号
   *   参数 optionalChecker返回值是元组(Boolean, int ),并且是Option类型的,默认值是None,作用是当异常时,将原始数据传回调用方
   *   
   *  向path上更新data数据和expectVersion版本号
   */
  def conditionalUpdatePersistentPath(client: ZkClient, path: String, data: String, expectVersion: Int,
    optionalChecker:Option[(ZkClient, String, String) => (Boolean,Int)] = None): (Boolean, Int) = {
    try {
      //向path写入data数据和期望的版本
      val stat = client.writeDataReturnStat(path, data, expectVersion)
      debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion)
    } catch {
      //版本号异常,当发现我们传入的optionalChecker方法,该方法是当异常时,将原始数据传回调用方
      case e1: ZkBadVersionException =>
        optionalChecker match {
          case Some(checker) => return checker(client, path, data)
          case _ => debug("Checker method is not passed skipping zkData match")
        }
        warn("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
          expectVersion, e1.getMessage))
        (false, -1)
      case e2: Exception =>
        warn("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
          expectVersion, e2.getMessage))
        (false, -1)
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the current
   * version is not the expected version, etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
   *  持久化的更新path下的data内容,并且仅仅path存在的情况下才允许该操作,如果path不存在则抛异常
   */
  def conditionalUpdatePersistentPathIfExists(client: ZkClient, path: String, data: String, expectVersion: Int): (Boolean, Int) = {
    try {
      val stat = client.writeDataReturnStat(path, data, expectVersion)
      debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion)
    } catch {
      case nne: ZkNoNodeException => throw nne
      case e: Exception =>
        error("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
          expectVersion, e.getMessage))
        (false, -1)
    }
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parrent directory if necessary. Never throw NodeExistException.
   * 更新临时节点上的数据,如果节点不存在,则创建该节点,并且写入数据
   */
  def updateEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
      case e2: Throwable => throw e2
    }
  }

  //删除该path
  def deletePath(client: ZkClient, path: String): Boolean = {
    try {
      client.delete(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
        false
      case e2: Throwable => throw e2
    }
  }

  //递归删除该path
  def deletePathRecursive(client: ZkClient, path: String) {
    try {
      client.deleteRecursive(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2: Throwable => throw e2
    }
  }

  //通过zookeeper连接字符串去递归删除path
  def maybeDeletePath(zkUrl: String, dir: String) {
    try {
      val zk = new ZkClient(zkUrl, 30*1000, 30*1000, ZKStringSerializer)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case _: Throwable => // swallow
    }
  }

  //读取path下的数据内容,该数据下面一定不会是null
  def readData(client: ZkClient, path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = client.readData(path, stat)
    (dataStr, stat)
  }

  //读取该path下的数据内容,可能最后结果是null
  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
                        (Some(client.readData(path, stat)), stat)
                      } catch {
                        case e: ZkNoNodeException =>
                          (None, stat)
                        case e2: Throwable => throw e2
                      }
    dataAndStat
  }

  //获取该path下面的一级子节点集合,该父节点必须存在
  def getChildren(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    client.getChildren(path)
  }

  //获取该path下面的一级子节点集合,该父节点可能不存在
  def getChildrenParentMayNotExist(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    try {
      client.getChildren(path)
    } catch {
      case e: ZkNoNodeException => return Nil
      case e2: Throwable => throw e2
    }
  }

  /**
   * Check if the given path exists
   * 校验path是否存在
   */
  def pathExists(client: ZkClient, path: String): Boolean = {
    client.exists(path)
  }

  //读取所有brokers节点,并且创建Broker节点对象
  def getCluster(zkClient: ZkClient) : Cluster = {
    val cluster = new Cluster
    //获取所有的节点集合
    val nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath)
    for (node <- nodes) {
      val brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node)._1
      cluster.add(Broker.createBroker(node.toInt, brokerZKString))
    }
    cluster
  }

  def getPartitionLeaderAndIsrForTopics(zkClient: ZkClient, topicAndPartitions: Set[TopicAndPartition])
  : mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = {
    val ret = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    for(topicAndPartition <- topicAndPartitions) {
      ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(leaderIsrAndControllerEpoch) => ret.put(topicAndPartition, leaderIsrAndControllerEpoch)
        case None =>
      }
    }
    ret
  }

  /**
   * 1.读取/brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
   * 2.解析内容,返回映射关系
   * 返回值 Map[TopicAndPartition, Seq[Int]] key是topic-partition,value是该partition都在哪些节点有备份
   */
  def getReplicaAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]] = {
    val ret = new mutable.HashMap[TopicAndPartition, Seq[Int]]
    topics.foreach { topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      jsonPartitionMapOpt match {
        
        case Some(jsonPartitionMap) =>
          Json.parseFull(jsonPartitionMap) match {
            case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
              case Some(repl)  =>
                val replicaMap = repl.asInstanceOf[Map[String, Seq[Int]]]
                for((partition, replicas) <- replicaMap){
                  ret.put(TopicAndPartition(topic, partition.toInt), replicas)
                  debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, partition, replicas))
                }
              case None =>
            }
            case None =>
          }
        case None =>
      }
    }
    ret
  }

  /**
   * 1.读取/brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
   * 2.解析内容,返回映射关系
   * 
   * 返回key是topic,value的key是该topic的partition,value是该partition对应的brokerId集合
   */
  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    //返回key是topic,value的key是该topic的partition,value是该partition对应的brokerId集合
    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
    topics.foreach{ topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      val partitionMap = jsonPartitionMapOpt match {
        
        case Some(jsonPartitionMap) =>
          Json.parseFull(jsonPartitionMap) match {
            case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
              case Some(replicaMap) =>
                val m1 = replicaMap.asInstanceOf[Map[String, Seq[Int]]]
                m1.map(p => (p._1.toInt, p._2))
              case None => Map[Int, Seq[Int]]()
            }
            case None => Map[Int, Seq[Int]]()
          }
        case None => Map[Int, Seq[Int]]()
      }
      debug("Partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
      ret += (topic -> partitionMap)
    }
    ret
  }

  /**
   * 1.getPartitionAssignmentForTopics(zkClient, topics) 读取/brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
   *   返回   Map,key是topic,value的key是该topic的partition,value是该partition对应的brokerId集合
   * 返回key是topic,value是该topic对应的partition集合.并且partition集合是按照顺序排好序l
   */
  def getPartitionsForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
    getPartitionAssignmentForTopics(zkClient, topics).map { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1//topic
      val partitionMap = topicAndPartitionMap._2//该topic的partition,value是该partition对应的brokerId集合
      debug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
      (topic -> partitionMap.keys.toSeq.sortWith((s,t) => s < t))
    }
  }

  def getPartitionsBeingReassigned(zkClient: ZkClient): Map[TopicAndPartition, ReassignedPartitionsContext] = {
    // read the partitions and their new replica list
    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, ReassignPartitionsPath)._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        val reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap)
        reassignedPartitions.map(p => (p._1 -> new ReassignedPartitionsContext(p._2)))
      case None => Map.empty[TopicAndPartition, ReassignedPartitionsContext]
    }
  }

  // Parses without deduplicating keys so the the data can be checked before allowing reassignment to proceed
  def parsePartitionReassignmentDataWithoutDedup(jsonData: String): Seq[(TopicAndPartition, Seq[Int])] = {
    Json.parseFull(jsonData) match {
      case Some(m) =>
        m.asInstanceOf[Map[String, Any]].get("partitions") match {
          case Some(partitionsSeq) =>
            partitionsSeq.asInstanceOf[Seq[Map[String, Any]]].map(p => {
              val topic = p.get("topic").get.asInstanceOf[String]
              val partition = p.get("partition").get.asInstanceOf[Int]
              val newReplicas = p.get("replicas").get.asInstanceOf[Seq[Int]]
              TopicAndPartition(topic, partition) -> newReplicas
            })
          case None =>
            Seq.empty
        }
      case None =>
        Seq.empty
    }
  }

  def parsePartitionReassignmentData(jsonData: String): Map[TopicAndPartition, Seq[Int]] = {
    parsePartitionReassignmentDataWithoutDedup(jsonData).toMap
  }

  def parseTopicsData(jsonData: String): Seq[String] = {
    var topics = List.empty[String]
    Json.parseFull(jsonData) match {
      case Some(m) =>
        m.asInstanceOf[Map[String, Any]].get("topics") match {
          case Some(partitionsSeq) =>
            val mapPartitionSeq = partitionsSeq.asInstanceOf[Seq[Map[String, Any]]]
            mapPartitionSeq.foreach(p => {
              val topic = p.get("topic").get.asInstanceOf[String]
              topics ++= List(topic)
            })
          case None =>
        }
      case None =>
    }
    topics
  }

  def getPartitionReassignmentZkData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): String = {
    Json.encode(Map("version" -> 1, "partitions" -> partitionsToBeReassigned.map(e => Map("topic" -> e._1.topic, "partition" -> e._1.partition,
                                                                                          "replicas" -> e._2))))
  }

  def updatePartitionReassignmentData(zkClient: ZkClient, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]) {
    val zkPath = ZkUtils.ReassignPartitionsPath
    partitionsToBeReassigned.size match {
      case 0 => // need to delete the /admin/reassign_partitions path
        deletePath(zkClient, zkPath)
        info("No more partitions need to be reassigned. Deleting zk path %s".format(zkPath))
      case _ =>
        val jsonData = getPartitionReassignmentZkData(partitionsToBeReassigned)
        try {
          updatePersistentPath(zkClient, zkPath, jsonData)
          info("Updated partition reassignment path with %s".format(jsonData))
        } catch {
          case nne: ZkNoNodeException =>
            ZkUtils.createPersistentPath(zkClient, zkPath, jsonData)
            debug("Created path %s with %s for partition reassignment".format(zkPath, jsonData))
          case e2: Throwable => throw new AdminOperationException(e2.toString)
        }
    }
  }

  def getPartitionsUndergoingPreferredReplicaElection(zkClient: ZkClient): Set[TopicAndPartition] = {
    // read the partitions and their new replica list
    val jsonPartitionListOpt = readDataMaybeNull(zkClient, PreferredReplicaLeaderElectionPath)._1
    jsonPartitionListOpt match {
      
      case Some(jsonPartitionList) => PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(jsonPartitionList)
      case None => Set.empty[TopicAndPartition]
    }
  }

  def deletePartition(zkClient : ZkClient, brokerId: Int, topic: String) {
    val brokerIdPath = BrokerIdsPath + "/" + brokerId
    zkClient.delete(brokerIdPath)
    val brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId
    zkClient.delete(brokerPartTopicPath)
  }

  //返回该group下的所有消费者名称集合
  def getConsumersInGroup(zkClient: ZkClient, group: String): Seq[String] = {
    val dirs = new ZKGroupDirs(group)
    getChildren(zkClient, dirs.consumerRegistryDir)
  }

  //返回属于该消费者组的topic与消费者集合映射
  def getConsumersPerTopic(zkClient: ZkClient, group: String, excludeInternalTopics: Boolean) : mutable.Map[String, List[ConsumerThreadId]] = {
    val dirs = new ZKGroupDirs(group)
    //获取该group下所有的消费者
    val consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir)
    
    //key是topic,value是该topic消费的ConsumerThreadId集合
    val consumersPerTopicMap = new mutable.HashMap[String, List[ConsumerThreadId]]
    for (consumer <- consumers) {//循环每一个消费者
      //获取该消费者可以消费哪些topic,以及有多少个线程可以去消费该topic
      val topicCount = TopicCount.constructTopicCount(group, consumer, zkClient, excludeInternalTopics)
      //循环每一个元组,即topic、消费该topic个多少个partition
      for ((topic, consumerThreadIdSet) <- topicCount.getConsumerThreadIdsPerTopic) {
        for (consumerThreadId <- consumerThreadIdSet)
          consumersPerTopicMap.get(topic) match {
                    /**
合并List
一个叫做“:::”的方法，可以把两个List连接在一起。

val oneTwo = List(1, 2)
val threeFour = List(3, 4)
val oneTwoThreeFour = oneTwo ::: threeFour // List(1, 2, 3, 4)

还有一个双冒号“::”的方法用来连接一个元素和List，这个元素会在List的最前端：

val twoThree = List(2, 3)
val oneTwoThree = 1 :: twoThree // List(1, 2, 3)
           */
            case Some(curConsumers) => consumersPerTopicMap.put(topic, consumerThreadId :: curConsumers)
            case _ => consumersPerTopicMap.put(topic, List(consumerThreadId))
          }
      }
    }
    
    //重新排序
    for ( (topic, consumerList) <- consumersPerTopicMap )
      consumersPerTopicMap.put(topic, consumerList.sortWith((s,t) => s < t))
    consumersPerTopicMap
  }

  /**
   * This API takes in a broker id, queries zookeeper for the broker metadata and returns the metadata for that broker
   * or throws an exception if the broker dies before the query to zookeeper finishes
   * @param brokerId The broker id
   * @param zkClient The zookeeper client connection
   * @return An optional Broker object encapsulating the broker metadata
   * 获取/brokers/ids/${brokerId}的内容.即该brokerId对应的host和part
   * 最后组装成Broker对象返回
   */
  def getBrokerInfo(zkClient: ZkClient, brokerId: Int): Option[Broker] = {
    ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
      
      case Some(brokerInfo) => Some(Broker.createBroker(brokerId, brokerInfo))
      case None => None
    }
  }

  //获取所有的topic集合
  def getAllTopics(zkClient: ZkClient): Seq[String] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null)
      Seq.empty[String]
    else
      topics
  }

  //获取所有的topic-partition集合
  def getAllPartitions(zkClient: ZkClient): Set[TopicAndPartition] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null) Set.empty[TopicAndPartition]
    else {
      topics.map { topic =>
        getChildren(zkClient, getTopicPartitionsPath(topic)).map(_.toInt).map(TopicAndPartition(topic, _))
      }.flatten.toSet
    }
  }
}

//zookeeper的内容序列化和反序列化
object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

  /**
    * /consumers
    * /consumers/${group}
    * /consumers/${group}/ids
   */
class ZKGroupDirs(val group: String) {
  //return /consumers
  def consumerDir = ZkUtils.ConsumersPath
  //return /consumers/${group}
  def consumerGroupDir = consumerDir + "/" + group
  //return /consumers/${group}/ids
  def consumerRegistryDir = consumerGroupDir + "/ids"
}

/**
 * /consumers/${group}/offsets/${topic}
 * /consumers/${owners}/offsets/${topic}
 */
class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
  //return /consumers/${group}/offsets/${topic}
  def consumerOffsetDir = consumerGroupDir + "/offsets/" + topic
  //return /consumers/${owners}/offsets/${topic}
  def consumerOwnerDir = consumerGroupDir + "/owners/" + topic
}


class ZKConfig(props: VerifiableProperties) {
  /** ZK host string */
  val zkConnect = props.getString("zookeeper.connect")

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = props.getInt("zookeeper.session.timeout.ms", 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = props.getInt("zookeeper.connection.timeout.ms",zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = props.getInt("zookeeper.sync.time.ms", 2000)
}
