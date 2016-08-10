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
package kafka.cluster

import kafka.common._
import kafka.admin.AdminUtils
import kafka.utils._
import kafka.api.{PartitionStateInfo, LeaderAndIsr}
import kafka.log.LogConfig
import kafka.server.{LogOffsetMetadata, OffsetManager, ReplicaManager}
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet

import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.utils.Utils.{inReadLock,inWriteLock}
import scala.collection.immutable.Set

import com.yammer.metrics.core.Gauge


/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 * kafka还可以配置partitions需要备份的个数(replicas),每个partition将会被备份到多台机器上,以提高可用性.
 * 每一个Partition是由topic决定的,即一个topic对应多个Partition,分别编号为partitionId
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  private val localBrokerId = replicaManager.config.brokerId//本地节点ID
  private val logManager = replicaManager.logManager//本地的LogManager
  private val zkClient = replicaManager.zkClient//本地的zookeeper
  //key是partition备份所在节点ID,value是对应的备份对象Replica,即备份partition的映射关系
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1 //leader选举的次数
  
  @volatile var leaderReplicaIdOpt: Option[Int] = None//该partition对应的leader节点
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]//已经同步的备份集合,只有partition的leader节点需要知道该属性
  
  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  //备份节点是否是本地机器
  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    Map("topic" -> topic, "partition" -> partitionId.toString)
  )

  /**
   * 判断当前partition是否没有同步完
   * true表示该partition的所有配分文件没有全部同步完成
   * 1.寻找该partition的leader
   * 2.该leader中所有分配的备份对象全部同步完成
   */
  def isUnderReplicated(): Boolean = {
    leaderReplicaIfLocal() match {
      case Some(_) => inSyncReplicas.size < assignedReplicas.size
      case None => false
    }
  }

  //获取该partition在replicaId节点上的备份,如果不存在,则创建一个备份对象返回
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    val replicaOpt = getReplica(replicaId)//获取该partition在replicaId节点上的备份对象
    replicaOpt match {
      case Some(replica) => replica
      case None =>
        if (isReplicaLocal(replicaId)) {//在本地创建一个log文件
          val config = LogConfig.fromProps(logManager.defaultConfig.toProps, AdminUtils.fetchTopicConfig(zkClient, topic))//创建topic的配置文件对象
          //为该topic-partition创建一个LOG对象,用于存储文件内容
          val log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          //key是log磁盘根目录,value是replication-offset-checkpoint文件
          //获取replication-offset-checkpoint文件对象OffsetCheckpoint
          val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath) //该值表示该partition所有的同步节点集合至少也同步到什么位置了
          val offsetMap = checkpoint.read //读取每一个topic-partition已经同步到偏移量
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            warn("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset) //设置偏移量,找到已经同步的最小的位置
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
        } else {
          //创建一个远程Replica对象
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }

  //获取该partition在replicaId节点上的备份对象
  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }

  //如果本地是备份的partition的leader,则获取该备份对象Replica,否则返回null
  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderReplicaIdOpt match {
      case Some(leaderReplicaId) =>
        if (leaderReplicaId == localBrokerId)
          getReplica(localBrokerId)
        else
          None
      case None => None
    }
  }

  //为该partition添加一组备份映射
  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  //获取该partition对应的全部备份对象
  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  //移除该partition在某一个节点的备份数据
  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  //删除该partition对应的文件
  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  //返回leader选举的次数
  def getLeaderEpoch(): Int = {
    return this.leaderEpoch
  }

  /**
   *  Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset from the time when this broker was the leader last time)
   *  and setting the new leader and ISR
   *  说明该partition在该节点是leader了
   *
   *  更新三大指标
   *  1.leader节点为本地节点
   *  2.备份节点集合
   *  3.同步节点集合
   */
  def makeLeader(controllerId: Int,//controller节点
                 partitionStateInfo: PartitionStateInfo,//此时该partition的leader详细信息
                 correlationId: Int,//请求关联的ID
                 offsetManager: OffsetManager): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.allReplicas //所有的备份节点ID集合
      val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr//LeaderAndIsr对象
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch //controller选举次数
      
      //更新内存映射关系
      // add replicas that are new 在partition对象内存中,映射新的Replica集合关系
      allReplicas.foreach(replica => getOrCreateReplica(replica)) //将备份节点ID集合转换成Replica集合,没有则添加,有则返回
      val newInSyncReplicas = leaderAndIsr.isr.map(r => getOrCreateReplica(r)).toSet //将同步节点集合ID 转换成Replica集合,没有则添加,有则返回
      // remove assigned replicas that have been removed by the controller
      //Set[Replica]中仅仅获取brokerId集合,然后从该集合中移除allReplicas集合内容.则表示已经删除的partition备份数据,因此调用removeReplica(_)移除
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      
      inSyncReplicas = newInSyncReplicas //获取要同步的集合
      leaderEpoch = leaderAndIsr.leaderEpoch //设置leader选举次数
      zkVersion = leaderAndIsr.zkVersion
      leaderReplicaIdOpt = Some(localBrokerId) //本地节点就是leader节点
      // construct the high watermark metadata for the new leader replica
      val newLeaderReplica = getReplica().get //获取本地 的Replica对象
      newLeaderReplica.convertHWToLocalOffsetMetadata()
      // reset log end offset for remote replicas 重新设置远程的replica对象的结束位置
      //因为本节点已经是该partiiton的leader节点了,因此本节点该partition的所有的follow都要重置偏移量
      assignedReplicas.foreach(r => if (r.brokerId != localBrokerId) r.logEndOffset = LogOffsetMetadata.UnknownOffsetMetadata)
      // we may need to increment high watermark since ISR could be down to 1
      maybeIncrementLeaderHW(newLeaderReplica)
      if (topic == OffsetManager.OffsetsTopicName) //kafka内部的topic,单独加载
        offsetManager.loadOffsetsFromLog(partitionId)
      true
    }
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  使local replica变化为 follower节点,更新new leader引用,以及将ISR集合设置为空集合
   *  If the leader replica id does not change, return false to indicate the replica manager
   *  如果leader节点没有被更改,则返回false给replica manager
   *  说明该节点此时是follow节点
   *
   *  更新三大指标
   *  1.leader节点
   *  2.备份节点集合
   *  3.同步节点集合,因为是follow节点,不需要知道有哪些同步节点,因此设置为空集合即可
   */
  def makeFollower(controllerId: Int,//controller节点ID
                   partitionStateInfo: PartitionStateInfo,//此时poartition的leader详细信息
                   correlationId: Int,//请求关联的ID
                   offsetManager: OffsetManager): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.allReplicas //获取此时所有的备份节点集合
      val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr //LeaderAndIsr对象
      val newLeaderBrokerId: Int = leaderAndIsr.leader //leader节点ID
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch //controller选举次数
      
      //更新内存映射关系
      // add replicas that are new  在partition对象内存中,映射新的Replica集合关系
      allReplicas.foreach(r => getOrCreateReplica(r)) //将备份节点ID集合--转换成备份节点对象集合
      // remove assigned replicas that have been removed by the controller
      //Set[Replica]中仅仅获取brokerId集合,然后从该集合中移除allReplicas集合内容.则表示已经删除的partition备份数据,因此调用removeReplica(_)移除
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_)) //有一些节点上已经不是备份节点了,则要删除掉
      inSyncReplicas = Set.empty[Replica] //因为不是leader,因此将需要同步的集合设置为空集合
      leaderEpoch = leaderAndIsr.leaderEpoch //设置leader的选举次数
      zkVersion = leaderAndIsr.zkVersion

      leaderReplicaIdOpt.foreach { leaderReplica =>
        if (topic == OffsetManager.OffsetsTopicName &&
           /* if we are making a leader->follower transition */
           leaderReplica == localBrokerId)
          offsetManager.clearOffsetsInPartition(partitionId)
      }

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {//如果leader节点本身就是newLeaderBrokerId,则说明不用重新设置,因此返回false,设置失败
        false
      }
      else {//设置leader节点ID为newLeaderBrokerId,设置成功
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  //对leader节点进行更新,并且扩展同步节点集合
  def updateLeaderHWAndMaybeExpandIsr(replicaId: Int) {
    inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      //获取leader节点,并且本地是leader节点
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          //获取leader节点成功
          val replica = getReplica(replicaId).get //返回一个备份节点对应的Replica对象
          val leaderHW = leaderReplica.highWatermark //返回Leader对应的LogOffsetMetadata对象
          // For a replica to get added back to ISR, it has to satisfy 3 conditions-
          //要满足三个条件,就将该备份加入到同步节点集合中,即当前不再同步节点集合、该节点在备份节点集合中、当前节点的位置已经达到了leader节点的位置了,则说明可以做为同步节点了
          // 1. It is not already in the ISR ,节点不在ISR同步节点里面存在
          // 2. It is part of the assigned replica list. See KAFKA-1097 该partition备份节点中包含replicaId节点,即replicaId节点确定有partition备份
          // 3. It's log end offset >= leader's high watermark 该partition的位置>=leader的位置
          if (!inSyncReplicas.contains(replica) &&
            assignedReplicas.map(_.brokerId).contains(replicaId) &&
            replica.logEndOffset.offsetDiff(leaderHW) >= 0) {
            // expand ISR 进行同步备份节点集合
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                 .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache 仅仅扩展同步节点集合
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }
          maybeIncrementLeaderHW(leaderReplica)
        case None => // nothing to do if no longer leader 如果不是leader节点,则什么也不做
      }
    }
  }

  /**
   * @requiredOffset 表示要求每一个备份文件必须要大于该阀值才作为可用备份
   * @requiredAcks 要求有这些数量的合法的备份数才是true
   *
   * 因为leader节点会记录每一个同步节点同步到哪个位置了
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long, requiredAcks: Int): (Boolean, Short) = {
    leaderReplicaIfLocal() match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference 保持当前的同步节点集合
        val curInSyncReplicas = inSyncReplicas
        
        //计算所有备份数据中大于给定参数requiredOffset偏移量的备份数据集合数量
        val numAcks = curInSyncReplicas.count(r => {
          if (!r.isLocal)
            r.logEndOffset.messageOffset >= requiredOffset
          else
            true /* also count the local (leader) replica */
        })

        //表示partition的最小同步数量,即达到该数量的备份数,就可以认为是成功备份了
        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        trace("%d/%d acks satisfied for %s-%d".format(numAcks, requiredAcks, topic, partitionId))
        //requiredAcks < 0 说明没有设置要多少个备份数
        if (requiredAcks < 0 && leaderReplica.highWatermark.messageOffset >= requiredOffset ) {
          /*
          * requiredAcks < 0 means acknowledge after all replicas in ISR
          * are fully caught up to the (local) leader's offset
          * corresponding to this produce request.
          *
          * minIsr means that the topic is configured not to accept messages
          * if there are not enough replicas in ISR
          * in this scenario the request was already appended locally and
          * then added to the purgatory before the ISR was shrunk
          */
          if (minIsr <= curInSyncReplicas.size) {//符合的备份数据大于最小备份阀值,因此返回正常
            (true, ErrorMapping.NoError)
          } else {
            (true, ErrorMapping.NotEnoughReplicasAfterAppendCode) //说明没有足够多的partition备份节点去备份数据
          }
        } else if (requiredAcks > 0 && numAcks >= requiredAcks) {//有足够的备份数量,因为这部分说明已经设置了需要多少个备份
          (true, ErrorMapping.NoError)
        } else
          (false, ErrorMapping.NoError)
      case None =>
        (false, ErrorMapping.NotLeaderForPartitionCode)
    }
  }

  /**
   * There is no need to acquire the leaderIsrUpdate lock here since all callers of this private API acquire that lock
   * @param leaderReplica leader节点的Replica对象
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica) {
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset) //获取每一个follow对象的位置
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering) //获取最小值
    val oldHighWatermark = leaderReplica.highWatermark
    if(oldHighWatermark.precedes(newHighWatermark)) {//true:当前的 比 参数小,即older比新的要小
      leaderReplica.highWatermark = newHighWatermark
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      // some delayed requests may be unblocked after HW changed
      val requestKey = new TopicAndPartition(this.topic, this.partitionId)
      replicaManager.unblockDelayedFetchRequests(requestKey)
      replicaManager.unblockDelayedProduceRequests(requestKey)
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
    }
  }

  /**
   * 返回卡住的同步对象集合
   * 所谓卡住的原因是:1.长时间没有从leader收到同步信息 2.收到的leader的同步信息数据较少
   * @replicaMaxLagTimeMs 表示最长时间不能从leader接收信息阀值
   * @replicaMaxLagMessages 表示从leader节点同步数据的最大字节长度阀值
   * 
   * 该方法表示收缩同步集合,因为有一些同步节点有问题,导致不再向该集合发送同步数据
   *  注意；只有leader节点才能去查看哪些同步节点没有成功
   *
   *  该类表示收缩同步节点集合,Shrink表示收缩的意思
   */
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long,  replicaMaxLagMessages: Long) {
    inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          // 注意；只有leader节点才能去查看哪些同步节点没有成功
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs, replicaMaxLagMessages) //获取有卡住的备份集合
          if(outOfSyncReplicas.size > 0) {//说明有同步节点没跟上进度
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas //刨除有问题的备份集合,剩余可用的集合
            assert(newInSyncReplicas.size > 0)
            //打印日志,要收缩该topic-partition的同步集合,原来同步集合是什么,现在同步集合是什么
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache 向zookeeper更新新的同步集合
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(leaderReplica)
            replicaManager.isrShrinkRate.mark()
          }
        case None => // do nothing if no longer leader 不是leader,不做任何事情
      }
    }
  }

  /**
   * 返回卡住的同步对象集合
   * 所谓卡住的原因是:1.长时间没有从leader收到同步信息 2.收到的leader的同步信息数据较少
   * @leaderReplica 表示leader的备份对象
   * @keepInSyncTimeMs 表示最长时间不能从leader接收信息阀值
   * @keepInSyncMessages 表示从leader节点同步数据的最大字节长度阀值
   * 返回值是有问题的备份集合
   *
   * 注意；只有leader节点才能去查看哪些同步节点没有成功
   */
  def getOutOfSyncReplicas(leaderReplica: Replica, keepInSyncTimeMs: Long, keepInSyncMessages: Long): Set[Replica] = {
    /**
     * there are two cases that need to be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for keepInSyncTimeMs ms,
     *                     the follower is stuck and should be removed from the ISR
     *                     表示有一段时间内没有同步信息了
     * 2. Slow followers: If the leo of the slowest follower is behind the leo of the leader by keepInSyncMessages, the
     *                     follower is not catching up and should be removed from the ISR
     *                     表示同步太慢
     **/
    val leaderLogEndOffset = leaderReplica.logEndOffset
    val candidateReplicas = inSyncReplicas - leaderReplica //刨除leader的同步集合 Set[Replica]
    // Case 1 above 查找不卡住的备份对象,即长时间没有同步信息
    val stuckReplicas = candidateReplicas.filter(r => (time.milliseconds - r.logEndOffsetUpdateTimeMs) > keepInSyncTimeMs)
    if(stuckReplicas.size > 0)
      debug("Stuck replicas for partition [%s,%d] are %s".format(topic, partitionId, stuckReplicas.map(_.brokerId).mkString(",")))
    // Case 2 above 已经落后leader的数据已经很久了
    val slowReplicas = candidateReplicas.filter(r =>
      r.logEndOffset.messageOffset >= 0 &&
      leaderLogEndOffset.messageOffset - r.logEndOffset.messageOffset > keepInSyncMessages)
    if(slowReplicas.size > 0)
      debug("Slow replicas for partition [%s,%d] are %s".format(topic, partitionId, slowReplicas.map(_.brokerId).mkString(",")))
    stuckReplicas ++ slowReplicas
  }

  //向本地的partition的leader中追加一条ByteBufferMessageSet信息
  def appendMessagesToLeader(messages: ByteBufferMessageSet, requiredAcks: Int=0) = {
    inReadLock(leaderIsrUpdateLock) {
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {//找到本地的leader的LOG对象
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get //leader节点log信息
          val minIsr = log.config.minInSyncReplicas //表示partition的最小同步数量,即达到该数量的备份数,就可以认为是成功备份了
          val inSyncSize = inSyncReplicas.size //等待同步集合

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          if (inSyncSize < minIsr && requiredAcks == -1) { //说明无论如何都没有办法满足最小同步备份数需求,则因此抛异常
            throw new NotEnoughReplicasException("Number of insync replicas for partition [%s,%d] is [%d], below required minimum [%d]"
              .format(topic,partitionId,minIsr,inSyncSize))
          }
          //追加信息到leader所在的日志文件中
          val info = log.append(messages, assignOffsets = true)
          // probably unblock some follower fetch requests since log end offset has been updated
          replicaManager.unblockDelayedFetchRequests(new TopicAndPartition(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          maybeIncrementLeaderHW(leaderReplica)
          info
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }
  }

  //对leader节点更新对应的同步节点集合
  private def updateIsr(newIsr: Set[Replica]) {
    //将新的ISR集合更新到zookeeper中
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    //更新/brokers/topics/${topic}/partitions/${partitionId}/state下的json内容信息
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)
    if(updateSucceeded) {
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  //topic和partition相同,就是同一个partition对象了
  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  //打印该topic-partition的leader节点以及分布了多少个备份节点，以及现在同步的备份节点有哪些
  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
