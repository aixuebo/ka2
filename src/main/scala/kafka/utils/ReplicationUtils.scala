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

import kafka.api.LeaderAndIsr
import kafka.controller.LeaderIsrAndControllerEpoch
import org.apache.zookeeper.data.Stat
import org.I0Itec.zkclient.ZkClient

import scala.Some
import scala.collection._

object ReplicationUtils extends Logging {

  //更新/brokers/topics/${topic}/partitions/${partitionId}/state下的json内容信息
  def updateLeaderAndIsr(zkClient: ZkClient, topic: String, partitionId: Int, newLeaderAndIsr: LeaderAndIsr, controllerEpoch: Int,
    zkVersion: Int): (Boolean,Int) = {
    debug("Updated ISR for partition [%s,%d] to %s".format(topic, partitionId, newLeaderAndIsr.isr.mkString(",")))
    //获取路径 /brokers/topics/${topic}/partitions/${partitionId}/state
    val path = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId)
    
    //向该路径写入json数据
    
    //生成待写入的json字符串
    val newLeaderData = ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    //将新json内容更新到path的内容上,并且设置期望的zkVersion版本号
    //checkLeaderAndIsrZkData参数作用是当异常时,将原始数据传回调用方
    ZkUtils.conditionalUpdatePersistentPath(zkClient, path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkData))
  }

  /**
   * 当实例化到zookeeper时,出现异常,则将原始的更新的内容和更新的path重新传回来
   * 
   * 因为失败了,因此重新查询该节点的信息,然后获取最新的信息后,重新将数据写入到节点
   */
  def checkLeaderAndIsrZkData(zkClient: ZkClient, path: String, expectedLeaderAndIsrInfo: String): (Boolean,Int) = {
    try {
      
      //重新获取信息,并且解析
      val writtenLeaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, path)//获取路径的内容
      val writtenLeaderOpt = writtenLeaderAndIsrInfo._1//获取的内容
      val writtenStat = writtenLeaderAndIsrInfo._2//获取的zookeeper状态
      val expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat)//用最新的版本号和待写入的数据生成期望的数据
      writtenLeaderOpt match {
        case Some(writtenData) =>
          val writtenLeader = parseLeaderAndIsr(writtenData, path, writtenStat)//获取最新的内容
          (expectedLeader,writtenLeader) match {
            case (Some(expectedLeader),Some(writtenLeader)) =>
              if(expectedLeader == writtenLeader)
                return (true,writtenStat.getVersion())//说明内容是一样的,就是版本号不一样,所以依然返回的是true
            case _ =>
          }
        case None =>
      }
    } catch {
      case e1: Exception =>
    }
    (false,-1)
  }

  //获取/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容,生成LeaderIsrAndControllerEpoch对象
  def getLeaderIsrAndEpochForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderIsrAndControllerEpoch] = {
    //获取/brokers/topics/${topic}/partitions/${partitionId}/state路径
    val leaderAndIsrPath = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition)
    //获取/brokers/topics/${topic}/partitions/${partitionId}/state路径下的内容
    val leaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, leaderAndIsrPath)
    val leaderAndIsrOpt = leaderAndIsrInfo._1
    val stat = leaderAndIsrInfo._2
    leaderAndIsrOpt match {
      case Some(leaderAndIsrStr) => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat)
      case None => None
    }
  }

  /**
   * @param leaderAndIsrStr ,/brokers/topics/${topic}/partitions/${partitionId}/state的内容,格式
   *        {leader:int,leader_epoch:int,isr:List[int],controller_epoch:int}
   * @param path,/brokers/topics/${topic}/partitions/${partitionId}/state的path路径
   * @param stat,zookeeper读取/brokers/topics/${topic}/partitions/${partitionId}/state节点后的状态
   * 解析/brokers/topics/${topic}/partitions/${partitionId}/state的内容  组装成 LeaderIsrAndControllerEpoch对象返回
   */
  private def parseLeaderAndIsr(leaderAndIsrStr: String, path: String, stat: Stat)
      : Option[LeaderIsrAndControllerEpoch] = {
    Json.parseFull(leaderAndIsrStr) match {
      case Some(m) =>
        val leaderIsrAndEpochInfo = m.asInstanceOf[Map[String, Any]]
        val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf[Int]
        val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf[Int]
        val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf[List[Int]]
        val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf[Int]
        val zkPathVersion = stat.getVersion
        debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s".format(leader, epoch,
          isr.toString(), zkPathVersion, path))
        Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))
      case None => None
    }
  }

}
