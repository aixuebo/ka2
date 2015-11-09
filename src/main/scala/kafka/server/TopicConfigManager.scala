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

import java.util.Properties
import scala.collection._
import kafka.log._
import kafka.utils._
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}

/**
 * This class initiates and carries out topic config changes.
 * 
 * It works as follows.
 * 
 * Config is stored under the path
 *   /brokers/topics/<topic_name>/config
 * This znode stores the topic-overrides for this topic (but no defaults) in properties format.
 * 
 * To avoid watching all topics for changes instead we have a notification path
 *   /brokers/config_changes
 * The TopicConfigManager has a child watch on this path.
 * 
 * To update a topic config we first update the topic config properties. Then we create a new sequential
 * znode under the change path which contains the name of the topic that was updated, say
 *   /brokers/config_changes/config_change_13321
 * This is just a notification--the actual config change is stored only once under the /brokers/topics/<topic_name>/config path.
 *   
 * This will fire a watcher on all brokers. This watcher works as follows. It reads all the config change notifications.
 * It keeps track of the highest config change suffix number it has applied previously. For any previously applied change it finds
 * it checks if this notification is larger than a static expiration time (say 10mins) and if so it deletes this notification. 
 * For any new changes it reads the new configuration, combines it with the defaults, and updates the log config 
 * for all logs for that topic (if any) that it has.
 * 
 * Note that config is always read from the config path in zk, the notification is just a trigger to do so. So if a broker is
 * down and misses a change that is fine--when it restarts it will be loading the full config anyway. Note also that
 * if there are two consecutive config changes it is possible that only the last one will be applied (since by the time the 
 * broker reads the config the both changes may have been made). In this case the broker would needlessly refresh the config twice,
 * but that is harmless.
 * 
 * On restart the config manager re-processes all notifications. This will usually be wasted work, but avoids any race conditions
 * on startup where a change might be missed between the initial config load and registering for change notifications.
 * 
 * 该类的作用是定期更新topic的配置信息
 * 
 * 参数changeExpirationMs表示过期时间,过期时间到了,则删除/config/changes节点下的某些子节点,即改动将不会生效
 */
class TopicConfigManager(private val zkClient: ZkClient,
                         private val logManager: LogManager,
                         private val changeExpirationMs: Long = 15*60*1000,
                         private val time: Time = SystemTime) extends Logging {
  private var lastExecutedChange = -1L//上次更新到哪个/config/changes子节点了
  
  /**
   * Begin watching for config changes
   */
  def startup() {
    //创建/config/changes节点
    ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.TopicConfigChangesPath)
    zkClient.subscribeChildChanges(ZkUtils.TopicConfigChangesPath, ConfigChangeListener)
    processAllConfigChanges()
  }
  
  /**
   * Process all config changes
   * 获取所有的topic的config更改节点集合
   */
  private def processAllConfigChanges() {
    //获取所有的topic的config更改节点集合
    val configChanges = zkClient.getChildren(ZkUtils.TopicConfigChangesPath)
    import JavaConversions._
    //对变更的信息进行排序,然后进行处理
    processConfigChanges((configChanges: mutable.Buffer[String]).sorted)
  }

  /**
   * Process the given list of config changes
   * 处理变更的config信息
   * 
   * 参数是/config/changes节点下的子节点名称集合
   */
  private def processConfigChanges(notifications: Seq[String]) {
    if (notifications.size > 0) {
      info("Processing config change notification(s)...")
      val now = time.milliseconds
      val logs = logManager.logsByTopicPartition.toBuffer
      //1.Map<TopicAndPartition, Log> 按照topic分组,即Map<Topic,Map<TopicAndPartition, Log>>
      //2.mapValues表示对map中的value进行处理,即对Map<TopicAndPartition, Log>进行处理,获取Map<TopicAndPartition, Log>中的每一个Log
      val logsByTopic = logs.groupBy(_._1.topic).mapValues(_.map(_._2))
      for (notification <- notifications) {
        val changeId = changeNumber(notification)//返回changId
        if (changeId > lastExecutedChange) {
          val changeZnode = ZkUtils.TopicConfigChangesPath + "/" + notification //获取子节点全路径
          val (jsonOpt, stat) = ZkUtils.readDataMaybeNull(zkClient, changeZnode)//读取变更节点的内容
          if(jsonOpt.isDefined) {
            val json = jsonOpt.get
            val topic = json.substring(1, json.length - 1) // hacky way to dequote 去除json的{}
            if (logsByTopic.contains(topic)) {
              /* combine the default properties with the overrides in zk to create the new LogConfig */
              val props = new Properties(logManager.defaultConfig.toProps)//读取默认配置
              props.putAll(AdminUtils.fetchTopicConfig(zkClient, topic))//读取topic的配置信息集合,/config/topics/topic
              val logConfig = LogConfig.fromProps(props)//
              for (log <- logsByTopic(topic))
                log.config = logConfig
              info("Processed topic config change %d for topic %s, setting new config to %s.".format(changeId, topic, props))
              purgeObsoleteNotifications(now, notifications)
            }
          }
          lastExecutedChange = changeId
        }
      }
    }
  }
  
  //删除一些过期节点
  private def purgeObsoleteNotifications(now: Long, notifications: Seq[String]) {
    for(notification <- notifications.sorted) {
      val (jsonOpt, stat) = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.TopicConfigChangesPath + "/" + notification)
      if(jsonOpt.isDefined) {
        val changeZnode = ZkUtils.TopicConfigChangesPath + "/" + notification
        if (now - stat.getCtime > changeExpirationMs) {
          debug("Purging config change notification " + notification)
          ZkUtils.deletePath(zkClient, changeZnode)
        } else {
          return
        }
      }
    }
  }
    
  /** get the change number from a change notification znode 
   * 参数name是以config_change_开头 + 数字,返回该数字
   * eg:config_change_5
   **/
  private def changeNumber(name: String): Long = name.substring(AdminUtils.TopicConfigChangeZnodePrefix.length).toLong
  
  /**
   * A listener that applies config changes to logs
   * 监控子节点变化事件
   * 监听/config/changes节点的子节点变化
   */
  object ConfigChangeListener extends IZkChildListener {
    override def handleChildChange(path: String, chillins: java.util.List[String]) {
      try {
        import JavaConversions._
        processConfigChanges(chillins: mutable.Buffer[String])
      } catch {
        case e: Exception => error("Error processing config change:", e)
      }
    }
  }

}