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

import joptsimple._
import java.util.Properties
import kafka.common.AdminCommandFailedException
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import scala.collection._
import scala.collection.JavaConversions._
import kafka.cluster.Broker
import kafka.log.LogConfig
import kafka.consumer.Whitelist
import kafka.server.OffsetManager
import org.apache.kafka.common.utils.Utils.formatAddress


object TopicCommand {

  def main(args: Array[String]): Unit = {
    
    val opts = new TopicCommandOptions(args)
    
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Create, delete, describe, or change a topic.")
    
    // should have exactly one action 必须要有增删改查命令
    val actions = Seq(opts.createOpt, opts.listOpt, opts.alterOpt, opts.describeOpt, opts.deleteOpt).count(opts.options.has _)
    if(actions != 1) 
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --create, --alter or --delete")

    opts.checkArgs()

    //连接zookeeper
    val zkClient = new ZkClient(opts.options.valueOf(opts.zkConnectOpt), 30000, 30000, ZKStringSerializer)

    //根据不同命令,操作不同逻辑
    try {
      if(opts.options.has(opts.createOpt))
        createTopic(zkClient, opts)
      else if(opts.options.has(opts.alterOpt))
        alterTopic(zkClient, opts)
      else if(opts.options.has(opts.listOpt))
        listTopics(zkClient, opts)
      else if(opts.options.has(opts.describeOpt))
        describeTopic(zkClient, opts)
      else if(opts.options.has(opts.deleteOpt))
        deleteTopic(zkClient, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      zkClient.close()
    }
  }

  //找到匹配的topic集合
  private def getTopics(zkClient: ZkClient, opts: TopicCommandOptions): Seq[String] = {
    val allTopics = ZkUtils.getAllTopics(zkClient).sorted //读取/brokers/topics下的子节点,获取全部topic名称
    if (opts.options.has(opts.topicOpt)) {
      val topicsSpec = opts.options.valueOf(opts.topicOpt)//topic名称,也可以是一个正则表达式
      val topicsFilter = new Whitelist(topicsSpec)
      allTopics.filter(topicsFilter.isTopicAllowed(_, excludeInternalTopics = false))//找到该正则表达式匹配的topic集合,该topic可以是kafka内部的topic
    } else
      allTopics
  }

  //创建一个topic
  def createTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)//对该topic进行读取配置信息
    if (opts.options.has(opts.replicaAssignmentOpt)) {
      val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, assignment, configs)
    } else {
      CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
      val partitions = opts.options.valueOf(opts.partitionsOpt).intValue //设置该topic有多少个partition
      val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue //设置topic的每一个partition要保留多少个备份
      AdminUtils.createTopic(zkClient, topic, partitions, replicas, configs)//真正创建该topic
    }
    println("Created topic \"%s\".".format(topic))
  }

  //更改topic信息
  def alterTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)//查询满足条件的topic集合
    //循环每一个topic
    topics.foreach { topic =>
      val configs = AdminUtils.fetchTopicConfig(zkClient, topic)//获取该topic已经存在的所有配置信息
      if(opts.options.has(opts.configOpt) || opts.options.has(opts.deleteConfigOpt)) {//有新增的配置或者删除的配置
        val configsToBeAdded = parseTopicConfigsToBeAdded(opts)//获取新增的配置集合
        val configsToBeDeleted = parseTopicConfigsToBeDeleted(opts)//获取要删除的配置key集合
        // compile the final set of configs
        configs.putAll(configsToBeAdded)//新增配置
        configsToBeDeleted.foreach(config => configs.remove(config))//删除这些配置key
        AdminUtils.changeTopicConfig(zkClient, topic, configs)//重新更新topic的配置信息
        println("Updated config for topic \"%s\".".format(topic))
      }
      if(opts.options.has(opts.partitionsOpt)) {//说明更改了partition数量
        if (topic == OffsetManager.OffsetsTopicName) {//不允许更改kafka内部的topic
          throw new IllegalArgumentException("The number of partitions for the offsets topic cannot be changed.")
        }
        println("WARNING: If partitions are increased for a topic that has a key, the partition " +
          "logic or ordering of the messages will be affected")
        val nPartitions = opts.options.valueOf(opts.partitionsOpt).intValue//新的partition数量
        val replicaAssignmentStr = opts.options.valueOf(opts.replicaAssignmentOpt)
        AdminUtils.addPartitions(zkClient, topic, nPartitions, replicaAssignmentStr, config = configs)
        println("Adding partitions succeeded!")
      }
    }
  }

  //打印符合要求的topic详细信息,并且该查询支持正则表达式
  def listTopics(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)//查看所有的topic
    for(topic <- topics) {
      if (ZkUtils.pathExists(zkClient,ZkUtils.getDeleteTopicPath(topic))) {
        println("%s - marked for deletion".format(topic))//标志该topic是删除的topic
      } else {
        println(topic)
      }
    }
  }

  //删除一些topic,支持正则表达式
  def deleteTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)//查找满足条件的topic集合
    if (topics.length == 0) {
      println("Topic %s does not exist".format(opts.options.valueOf(opts.topicOpt)))
    }

    //循环每一个topic
    topics.foreach { topic =>
      try {
        ZkUtils.createPersistentPath(zkClient, ZkUtils.getDeleteTopicPath(topic))//创建删除该topic的zookeeper节点
        println("Topic %s is marked for deletion.".format(topic))
        println("Note: This will have no impact if delete.topic.enable is not set to true.")
      } catch {
        case e: ZkNodeExistsException =>
          println("Topic %s is already marked for deletion.".format(topic))
        case e2: Throwable =>
          throw new AdminOperationException("Error while deleting topic %s".format(topic))
      }    
    }
  }

  //详细描述一些topic
  def describeTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)//查看要准备看的topic,也可以是正则表达式
    val reportUnderReplicatedPartitions = if (opts.options.has(opts.reportUnderReplicatedPartitionsOpt)) true else false
    val reportUnavailablePartitions = if (opts.options.has(opts.reportUnavailablePartitionsOpt)) true else false
    val reportOverriddenConfigs = if (opts.options.has(opts.topicsWithOverridesOpt)) true else false
    val liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).map(_.id).toSet//获取所有活着的节点id集合
    for (topic <- topics) {//循环每一个topic
      /**
       * ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic))返回Map[String, collection.Map[Int, Seq[Int]]]
       * 返回key是topic,value的key是该topic的partition,value是该partition对应的brokerId集合
       */
      ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic) match {
        case Some(topicPartitionAssignment) =>
          val describeConfigs: Boolean = !reportUnavailablePartitions && !reportUnderReplicatedPartitions //这两个参数都是false的时候,才打印配置文件
         val describePartitions: Boolean = !reportOverriddenConfigs//是false的时候才打印partition信息
          val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)//按照partition排序
          if (describeConfigs) {//打印配置信息
            val configs = AdminUtils.fetchTopicConfig(zkClient, topic)//读取该topic的配置信息
            if (!reportOverriddenConfigs || configs.size() != 0) {
              val numPartitions = topicPartitionAssignment.size//该topic的partition数量
              val replicationFactor = topicPartitionAssignment.head._2.size//第一个partition的备份数量
              println("Topic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:%s"
                .format(topic, numPartitions, replicationFactor, configs.map(kv => kv._1 + "=" + kv._2).mkString(",")))
              //打印topic有多少个partition,以及备份因子是多少,以及所有的配置信息
            }
          }
          if (describePartitions) {//打印描述信息
            for ((partitionId, assignedReplicas) <- sortedPartitions) {//sortedPartitions表示collection.Map[Int, Seq[Int]] 每一个partition对应一组该partition在哪些节点上存在
              val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionId)//获取正在同步的节点集合 List<Int>
              val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionId)//获取leader节点
              if ((!reportUnderReplicatedPartitions && !reportUnavailablePartitions) ||
                  (reportUnderReplicatedPartitions && inSyncReplicas.size < assignedReplicas.size) ||
                  (reportUnavailablePartitions && (!leader.isDefined || !liveBrokers.contains(leader.get)))) {
                print("\tTopic: " + topic)
                print("\tPartition: " + partitionId)
                print("\tLeader: " + (if(leader.isDefined) leader.get else "none"))//谁是该partition的leader节点
                print("\tReplicas: " + assignedReplicas.mkString(","))//都在哪些节点存在该partition信息
                println("\tIsr: " + inSyncReplicas.mkString(","))//谁是从节点
              }
            }
          }
        case None =>
          println("Topic " + topic + " doesn't exist!")
      }
    }
  }

  //格式化一个节点对象
  def formatBroker(broker: Broker) = broker.id + " (" + formatAddress(broker.host, broker.port) + ")"

  //读取配置文件,生成Properties对象
  def parseTopicConfigsToBeAdded(opts: TopicCommandOptions): Properties = {
    val configsToBeAdded = opts.options.valuesOf(opts.configOpt).map(_.split("""\s*=\s*"""))//每一个属性都是keyvalue形式
    require(configsToBeAdded.forall(config => config.length == 2),
      "Invalid topic config: all configs to be added must be in the format \"key=val\".")
    val props = new Properties
    configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).trim))
    LogConfig.validate(props)
    props
  }

  //读取要删除的配置属性key的集合
  def parseTopicConfigsToBeDeleted(opts: TopicCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfigOpt)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfigOpt).map(_.trim())
      val propsToBeDeleted = new Properties
      configsToBeDeleted.foreach(propsToBeDeleted.setProperty(_, ""))
      LogConfig.validateNames(propsToBeDeleted)
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  /**
   * 解析 当手动配置某一个topic-partition要将备份分配给哪些节点上去备份
   * @param replicaAssignmentList 格式broker_id_for_part1_replica1:broker_id_for_part1_replica2,broker_id_for_part2_replica1:broker_id_for_part2_replica2
   *  按照逗号拆分,每一段表示一个partition的备份节点集合
   *  一组备份节点集合按照:拆分
   */
  def parseReplicaAssignment(replicaAssignmentList: String): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")//先按照逗号拆分,拆分后就表示为第一个到第n个partition分配备份
    val ret = new mutable.HashMap[Int, List[Int]]()
    for (i <- 0 until partitionList.size) {//循环每一组配置
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)//表示该partition要去哪些节点去做备份
      val duplicateBrokers = Utils.duplicates(brokerList)//找到集合中有重复的key的集合
      if (duplicateBrokers.nonEmpty) //不允许有重复在同一个节点上创建备份的配置
        throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicateBrokers.mkString(",")))
      ret.put(i, brokerList.toList)//映射第i个partition在哪些节点映射
      if (ret(i).size != ret(0).size)//所有的partition的备份数量要相同,即都与第一个partition备份数量相同即可
        throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList)
    }
    ret.toMap
  }

  //topic的命令行参数
  class TopicCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])//连接哪个zookeeper

    //命令
    val listOpt = parser.accepts("list", "List all available topics.") //查看所有可用的topic集合
    val createOpt = parser.accepts("create", "Create a new topic.")//创建新的topic
    val deleteOpt = parser.accepts("delete", "Delete a topic")//删除一个topic
    val alterOpt = parser.accepts("alter", "Alter the configuration for the topic.")//更改一个topic
    val describeOpt = parser.accepts("describe", "List details for the given topics.")//查看一个topic详细配置信息
    val helpOpt = parser.accepts("help", "Print usage information.")//帮助命令

    val topicOpt = parser.accepts("topic", "The topic to be create, alter or describe. Can also accept a regular " +
                                           "expression except for --create option")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])//topic名称,也可以是一个正则表达式


    val nl = System.getProperty("line.separator")
    val configOpt = parser.accepts("config", "A topic configuration override for the topic being created or altered."  + 
                                                         "The following is a list of valid configurations: " + nl + LogConfig.ConfigNames.map("\t" + _).mkString(nl) + nl +  
                                                         "See the Kafka documentation for full details on the topic configs.")
                          .withRequiredArg
                          .describedAs("name=value")
                          .ofType(classOf[String])//配置文件,每一个属性都是key=value,如果要配置多个属性,要输入多次conf命令
    val deleteConfigOpt = parser.accepts("delete-config", "A topic configuration override to be removed for an existing topic (see the list of configurations under the --config option).")
                          .withRequiredArg
                          .describedAs("name")
                          .ofType(classOf[String])//要删除的配置属性,这个配置的是要删除的key
    val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
      "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected")
                           .withRequiredArg
                           .describedAs("# of partitions")
                           .ofType(classOf[java.lang.Integer])//设置该topic有多少个partition
    val replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created.")
                           .withRequiredArg
                           .describedAs("replication factor")
                           .ofType(classOf[java.lang.Integer])//设置topic的每一个partition要保留多少个备份

    /**
     * 格式broker_id_for_part1_replica1:broker_id_for_part1_replica2,broker_id_for_part2_replica1:broker_id_for_part2_replica2
     *  按照逗号拆分,每一段表示一个partition的备份节点集合
     *  一组备份节点集合按照:拆分
     */
    val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created or altered.")//手动配置partition分配到哪些节点上去备份
                           .withRequiredArg
                           .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                                        "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                           .ofType(classOf[String])//如何分配每一个partition到哪些节点上去备份

   //以下三个属性都是用于describing命令的,只要下面三个参数都是false的时候,会打印全部详细信息,包括topic的配置以及每一个partition的详细信息
    val reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                                                            "if set when describing topics, only show under replicated partitions")
    val reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                                                            "if set when describing topics, only show partitions whose leader is not available")
    val topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
                                                "if set when describing topics, only show topics that have overridden configs")

    val options = parser.parse(args : _*)

    //记录topic的命令集合
    val allTopicLevelOpts: Set[OptionSpec[_]] = Set(alterOpt, createOpt, describeOpt, listOpt)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      if (!options.has(listOpt) && !options.has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, configOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, deleteConfigOpt, allTopicLevelOpts -- Set(alterOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, partitionsOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicationFactorOpt, allTopicLevelOpts -- Set(createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt,
        allTopicLevelOpts -- Set(alterOpt, createOpt) + partitionsOpt + replicationFactorOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnderReplicatedPartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnavailablePartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnavailablePartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicsWithOverridesOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + reportUnavailablePartitionsOpt)
    }
  }
  
}
