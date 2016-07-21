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
package kafka.admin

import joptsimple.OptionParser
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.common.{TopicAndPartition, AdminCommandFailedException}
import collection._
import mutable.ListBuffer

//优先选举备份节点的leaderr命令
object PreferredReplicaLeaderElectionCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser

    /**
     * 格式
      {
     partitions:
          [
           {topic:"foll",partition:1},
           {topic:"foll",partition:2},

           {topic:"fol2",partition:1},
           {topic:"fol2",partition:2}
           ]
      }
     */
    val jsonFileOpt = parser.accepts("path-to-json-file", "The JSON file with the list of partitions " +
      "for which preferred replica leader election should be done, in the following format - \n" +
       "{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\n" +
      "Defaults to all existing partitions")
      .withRequiredArg
      .describedAs("list of partitions for which preferred replica leader election needs to be triggered")
      .ofType(classOf[String])

    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])//zookeeper连接器
      
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "This tool causes leadership for each partition to be transferred back to the 'preferred replica'," + 
                                                " it can be used to balance leadership among the servers.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)

    val zkConnect = options.valueOf(zkConnectOpt)
    var zkClient: ZkClient = null

    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      val partitionsForPreferredReplicaElection =
        if (!options.has(jsonFileOpt))
          ZkUtils.getAllPartitions(zkClient)//如果没有配置文件,则获取所有的topic-partition集合 Set[TopicAndPartition]
        else
          parsePreferredReplicaElectionData(Utils.readFileAsString(options.valueOf(jsonFileOpt)))

      val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkClient, partitionsForPreferredReplicaElection)
      preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
      println("Successfully started preferred replica election for partitions %s".format(partitionsForPreferredReplicaElection))
    } catch {
      case e: Throwable =>
        println("Failed to start preferred replica election")
        println(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  /**
   * 解析/admin/preferred_replica_election节点信息的内容,内容是一个map,格式{"partitions":[{topic=value,partition=value},{topic=value,partition=value}]},
   * 总格式整理:
   * partitions = List[Map[String, Any]]
   * 其中key包含 topic,partition
   * 返回值是所有的topic-partition集合,即Set[TopicAndPartition]
   */
  def parsePreferredReplicaElectionData(jsonString: String): immutable.Set[TopicAndPartition] = {
    Json.parseFull(jsonString) match {
      case Some(m) =>
        m.asInstanceOf[Map[String, Any]].get("partitions") match {
          case Some(partitionsList) =>
            val partitionsRaw = partitionsList.asInstanceOf[List[Map[String, Any]]]
            val partitions = partitionsRaw.map { p =>
              val topic = p.get("topic").get.asInstanceOf[String]
              val partition = p.get("partition").get.asInstanceOf[Int]
              TopicAndPartition(topic, partition)
            }
            val duplicatePartitions = Utils.duplicates(partitions)//校验partition不允许重复
            val partitionsSet = partitions.toSet
            if (duplicatePartitions.nonEmpty)
              throw new AdminOperationException("Preferred replica election data contains duplicate partitions: %s".format(duplicatePartitions.mkString(",")))
            partitionsSet
          case None => throw new AdminOperationException("Preferred replica election data is empty")
        }
      case None => throw new AdminOperationException("Preferred replica election data is empty")
    }
  }

  //将topic-partition集合写入到zookeeper中
  //写入到zookeeper即可,control会有监听该节点,进行异步的leader分配
  def writePreferredReplicaElectionData(zkClient: ZkClient,
                                        partitionsUndergoingPreferredReplicaElection: scala.collection.Set[TopicAndPartition]) {
    val zkPath = ZkUtils.PreferredReplicaLeaderElectionPath
    val partitionsList = partitionsUndergoingPreferredReplicaElection.map(e => Map("topic" -> e.topic, "partition" -> e.partition))
    val jsonData = Json.encode(Map("version" -> 1, "partitions" -> partitionsList))
    try {
      ZkUtils.createPersistentPath(zkClient, zkPath, jsonData)
      info("Created preferred replica election path with %s".format(jsonData))
    } catch {
      case nee: ZkNodeExistsException =>
        val partitionsUndergoingPreferredReplicaElection =
          PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(ZkUtils.readData(zkClient, zkPath)._1)
        throw new AdminOperationException("Preferred replica leader election currently in progress for " +
          "%s. Aborting operation".format(partitionsUndergoingPreferredReplicaElection))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }
}

/**
 * 为参数topic-partition集合选择一个leader节点
 */
class PreferredReplicaLeaderElectionCommand(zkClient: ZkClient, partitions: scala.collection.Set[TopicAndPartition])
  extends Logging {
  def moveLeaderToPreferredReplica() = {
    try {
      val validPartitions = partitions.filter(p => validatePartition(zkClient, p.topic, p.partition))//过滤掉合法的topic-partition
      PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, validPartitions)//将具体的内容写入到zookeeper中,监听的control会对该节点进行处理
    } catch {
      case e: Throwable => throw new AdminCommandFailedException("Admin command failed", e)
    }
  }

  def validatePartition(zkClient: ZkClient, topic: String, partition: Int): Boolean = {
    // check if partition exists 校验该partition是否存在
    val partitionsOpt = ZkUtils.getPartitionsForTopics(zkClient, List(topic)).get(topic) //获取该topic目前所有的partition集合
    partitionsOpt match {
      case Some(partitions) =>
        if(partitions.contains(partition)) {//判断参数partition是否存在
          true
        } else {
          error("Skipping preferred replica leader election for partition [%s,%d] ".format(topic, partition) +
            "since it doesn't exist")
          false
        }
      case None => error("Skipping preferred replica leader election for partition " +
        "[%s,%d] since topic %s doesn't exist".format(topic, partition, topic))
        false
    }
  }
}
