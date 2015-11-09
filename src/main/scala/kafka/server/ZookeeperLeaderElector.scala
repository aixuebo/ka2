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

import kafka.utils.ZkUtils._
import kafka.utils.Utils._
import kafka.utils.{Json, SystemTime, Logging}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 * 选举类
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,//当该节点变成leader时候执行该函数
                             onResigningAsLeader: () => Unit,//当重新设置leader选举时,执行该函数
                             brokerId: Int)
  extends LeaderElector with Logging {
  var leaderId = -1//当前leader是什么
  // create the election path in ZK, if one does not exist
  //如果path不存在则创建一个持久化的path,总之该方法确保path持久化存在,path是最后一个/之前的称之为path.
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    makeSurePersistentPathExists(controllerContext.zkClient, electionPath.substring(0, index))
    
  val leaderChangeListener = new LeaderChangeListener

  def startup {
    inLock(controllerContext.controllerLock) {
      //向zookeeper提交一个该节点的监听,当该节点数据有变化时,要通知
      controllerContext.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      elect
    }
  }

  //读取该path下的内容,查看谁是leader,然后返回该leader的broker的id
  private def getControllerID(): Int = {
    //读取该path下的内容,可能最后结果是null
    readDataMaybeNull(controllerContext.zkClient, electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1//说明该节点没有数据,则返回-1
    }
  }
    
  //返回该节点是否选举成功
  def elect: Boolean = {
    val timestamp = SystemTime.milliseconds.toString
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))
   
   leaderId = getControllerID //获取当前leader
    /* 
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition, 
     * it's possible that the controller has already been elected when we get here. This check will prevent the following 
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     * 如果leader已经存在了,则停止选举过程,返回该节点是否是leader即可
     */
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       return amILeader
    }

    //程序到这里,说明leaderId=-1,要进行选举
    try {
      createEphemeralPathExpectConflictHandleZKBug(controllerContext.zkClient, electionPath, electString, brokerId,
        (controllerString : String, leaderId : Any) => KafkaController.parseControllerId(controllerString) == leaderId.asInstanceOf[Int],
        controllerContext.zkSessionTimeout)
      info(brokerId + " successfully elected as leader")
      leaderId = brokerId//说明该节点被选举成功
      onBecomingLeader()//当该节点变成leader时候执行该函数
    } catch {
      case e: ZkNodeExistsException =>
        // If someone else has written the path, then 
        leaderId = getControllerID //说明已经有其他节点选举上了,则获取当前被选举的节点Id

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>//说明选举中出现异常,要重置
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        resign()
    }
    amILeader
  }

  def close = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  //重置
  def resign() = {
    leaderId = -1
    deletePath(controllerContext.zkClient, electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   * 监听path上的data数据的更改和删除事件
   * 
   * 参数controllerInfoString:是json格式,解析的内容是哪个broker节点是kafka主节点,返回该主节点的broker的id
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     * 更改leader
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      inLock(controllerContext.controllerLock) {
        leaderId = KafkaController.parseControllerId(data.toString)//返回更改后的leader
        info("New leader is %d".format(leaderId))//日志输出新leader
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
     * leader被删除,则重新选举           
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        if(amILeader)
          onResigningAsLeader()
        elect
      }
    }
  }
}
