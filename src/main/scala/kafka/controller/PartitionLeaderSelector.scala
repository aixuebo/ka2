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
package kafka.controller

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.utils.Logging
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.server.KafkaConfig

trait PartitionLeaderSelector {

  /**
   * @param topicAndPartition          The topic and partition whose leader needs to be elected
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper 当前leader和同步列表集合
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest. 返回新的leader和同步列表集合 以及 新的备份节点集合
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 *    isr as the new isr.
 *    至少有一个同步集合活着,从中拿出一个节点作为leader节点,并且活着的同步节点集合作为新的集合
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 如果配置属性是false,则抛异常
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 从活着的备份节点中拿去一个作为leader节点,并且同步集合中也只有一个leader节点存在
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * 如果没有备份节点集合活着,则抛NoReplicaOnlineException异常
 * Replicas to receive LeaderAndIsr request = live assigned replicas 备份节点集合 保留活着的备份节点集合
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {
  this.logIdent = "[OfflinePartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {//找到备份节点集合
      case Some(assignedReplicas) =>
        val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r)) //过滤,保留活着的备份节点集合
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))//过滤,保留活着的同步节点集合
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
        val newLeaderAndIsr = liveBrokersInIsr.isEmpty match {
          case true => //说明没有活着的同步节点集合
            // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
            // for unclean leader election.通过配置文件,优先选择unclean leader,确保是否允许
            if (!LogConfig.fromProps(config.props.props, AdminUtils.fetchTopicConfig(controllerContext.zkClient,
              topicAndPartition.topic)).uncleanLeaderElectionEnable) {//读取该topic的配置文件

              //打印日志,该partition中没有同步集合,但是集群是有其他xxx节点活着的,但是该partition的同步节点是yyy,yyy没有一个在xxx中
              throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                " ISR brokers are: [%s]".format(currentLeaderAndIsr.isr.mkString(",")))
            }

            //没有活着的同步节点存在,因此要从活着的备份节点中获取leader
            debug("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s"
              .format(topicAndPartition, liveAssignedReplicas.mkString(",")))
            liveAssignedReplicas.isEmpty match {
              case true =>
                //如果备份节点也是空.则抛异常
                throw new NoReplicaOnlineException(("No replica for partition " +
                  "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                  " Assigned replicas are: [%s]".format(assignedReplicas))
              case false =>
                //说明有备份节点存在,则获取第一个备份节点为leader节点
                ControllerStats.uncleanLeaderElectionRate.mark()
                val newLeader = liveAssignedReplicas.head
                warn("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss."
                     .format(topicAndPartition, newLeader, liveAssignedReplicas.mkString(",")))
                new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
            }
          case false => //说明同步节点集合有节点
            val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r)) //备份节点集合与同步节点集合 交集,即活着的备份节点集合 & 同步节点集合
            val newLeader = liveReplicasInIsr.head //第一个就是作为leader
            debug("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader."
                  .format(topicAndPartition, newLeader, liveBrokersInIsr.mkString(",")))

            //此时的同步节点就只有leader节点一个
            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
        }
        info("Selected new leader and ISR %s for offline partition %s".format(newLeaderAndIsr.toString(), topicAndPartition))
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
      //说明没有备份节点集合
        throw new NoReplicaOnlineException("Partition %s doesn't have replicas assigned to it".format(topicAndPartition))
    }
  }
}

/**
 * 此过程是管理员分配了若干个备份节点的时候,使用第一个备份节点作为leader节点
 * New leader = a live in-sync reassigned replica leader节点为活着的在同步列表中的备份节点
 * New isr = current isr 当前同步节点集合不变
 * Replicas to receive LeaderAndIsr request = reassigned replicas 备份节点集合不变
 */
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {
  this.logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas //获取管理员分配的新的备份节点集合
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

    //过滤当前分配的节点 必须是活着的节点  同时 也是同步列表中的节点
    val aliveReassignedInSyncReplicas = reassignedInSyncReplicas.filter(r => controllerContext.liveBrokerIds.contains(r) &&
                                                                             currentLeaderAndIsr.isr.contains(r))
    val newLeaderOpt = aliveReassignedInSyncReplicas.headOption //获取第一个节点作为leader节点
    newLeaderOpt match {
      case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
        currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
      case None =>
        reassignedInSyncReplicas.size match {
          case 0 =>
            throw new NoReplicaOnlineException("List of reassigned replicas for partition " +
              " %s is empty. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
          case _ =>
            throw new NoReplicaOnlineException("None of the reassigned replicas for partition " +
              "%s are in-sync with the leader. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
        }
    }
  }
}

/**
 * leader节点是管理员分配好的
 * New leader = preferred (first assigned) replica (if in isr and alive);如果首选备份节点集合中第一个节点是活着的节点,并且在同步列表中,则优先分配为leader节点
 * New isr = current isr; 不更改当前的同步集合
 * Replicas to receive LeaderAndIsr request = assigned replicas 不更改备份节点集合
 */
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector
with Logging {
  this.logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition) //备份节点集合
    val preferredReplica = assignedReplicas.head //第一个备份节点集合作为首选的leader节点
    // check if preferred replica is the current leader
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    if (currentLeader == preferredReplica) {//说明当前leader就是第一个备份节点
      throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                                                   .format(preferredReplica, topicAndPartition))
    } else {
      info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
        " Trigerring preferred replica leader election")
      // check if preferred replica is not the current leader and is alive and in the isr
      //说明第一个备份节点是活着的节点  同时 也是同步节点集合中的节点,因此将其设置为leader节点即可
      if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
          currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
      } else {
        throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica) +
          "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
      }
    }
  }
}

/**
 * New leader = replica in isr that's not being shutdown; 新的leader是同步列表中没有shutdown的
 * New isr = current isr - shutdown replica; 新的同步列表是当前同步列表 - 正在shutdown的
 * Replicas to receive LeaderAndIsr request = live assigned replicas 备份节点集合是活着的分配的备份节点集合
 * 当leader所在节点进行shutdown的时候,使用该选择器,重新选择leader
 */
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext)
        extends PartitionLeaderSelector
        with Logging {

  this.logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

    val currentLeader = currentLeaderAndIsr.leader

    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition) //当前的备份节点集合
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r)) //过滤,仅保持现在已知的节点集合

    val newIsr = currentLeaderAndIsr.isr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId)) //过滤同步节点集合,即必须在活着的节点中
    val newLeaderOpt = newIsr.headOption //第一个同步的节点作为leader节点
    newLeaderOpt match {
      case Some(newLeader) =>
        debug("Partition %s : current leader = %d, new leader = %d"
              .format(topicAndPartition, currentLeader, newLeader))
        (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1),
         liveAssignedReplicas)
      case None =>
        throw new StateChangeFailedException(("No other replicas in ISR %s for %s besides" +
          " shutting down brokers %s").format(currentLeaderAndIsr.isr.mkString(","), topicAndPartition, controllerContext.shuttingDownBrokerIds.mkString(",")))
    }
  }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
 * 本质上什么都没有做,只是返回了参数作为leader,partition对应的所有备份ID集合作为元组的第二个参数被返回
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  this.logIdent = "[NoOpLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
    //  controllerContext.partitionReplicaAssignment:key是topic-partition对象,value是该partition的备份的ID集合
    (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
  }
}
