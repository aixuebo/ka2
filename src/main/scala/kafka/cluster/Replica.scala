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

import kafka.log.Log
import kafka.utils.{SystemTime, Time, Logging}
import kafka.server.LogOffsetMetadata
import kafka.common.KafkaException

import java.util.concurrent.atomic.AtomicLong

//kafka还可以配置partitions需要备份的个数(replicas),每个partition将会被备份到多台机器上,以提高可用性.
/**
对于local replica, 需要记录highWatermarkValue，表示当前已经committed的数据
对于remote replica，需要记录logEndOffsetValue以及更新的时间

该对象表示一个备份,在brokerId节点备份Partition对象,即topic-partition的一个备份
 */

/**
 * @log 参数是该节点上存储该partition的日志文件,如果该参数有值,说明该节点是本地机器
 * @Replica 表示该Replica所在broker节点ID
 * 
 * 该对象表示一个partition的一个备份,因此他包含Partition对象
 *
 * 总结:
 * 1.Replica 表示一个partition的备份,因此对应哪个Partition就是参数partition的意义
 * 2.brokerId 表示在brokerId节点存放着一个该partition的备份
 * 3.initialHighWatermarkValue
 * 4.log: Option[Log] 如果该partition在本节点就存在一个备份,则肯定有一个日志跟着,因此该参数就存在内容,如果备份在本节点不存在,则该值就是null
 */
class Replica(val brokerId: Int,//该partition在哪个节点上的备份
              val partition: Partition,//属于哪个partition的备份对象
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,//该partition在本地的最高offset
              val log: Option[Log] = None) extends Logging {//该partition对应的本地log文件
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  /**
   * 如果该备份对象是leader节点对应的备份对象,则该值代表follower节点最小的同步已经同步到哪里了,例如该值是300,说明若干个follow节点,最小的也同步到300了
   * 如果该备份对象是follow节点对应的备份对象,则该值代表已经同步leader到什么位置了
   *
  leader节点在partition的maybeIncrementLeaderHW方法里面调用,更新所有follower节点最小的值为水印值,
  partition的四个操作都会更新该值:
  a.appendMessagesToLeader追加信息
  b.updateLeaderHWAndMaybeExpandIsr 每一次fetch请求发给leader的时候，leader都会知道follow节点请求的最小值是什么,因此更新该水印值
  c.maybeShrinkIsr,定期查看partition的哪些备份节点是否过期的时候，因为有过期的follow,因此会更新该水印值。
  d.makeLeader 当作为leader节点的时候,要重新计算该水印值。
   * follower节点是fetch请求时候,leader节点传回follower节点的，详细查看ReplicaManager的readMesasgeSet方法,该传回来的数据就是leader节点自己知道的所有follow节点上最小的请求序号
   */
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  //日志最后的偏移量位置,保存在所有的备份数据中
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  //对于本地的备份,该值表示日志的最后一个位置,对于远程的备份,该值表示抓取到了哪里,用于follower节点
  /**
   * 如果该备份对象是本地节点对应的有log的备份对象,则该值表示在本地log文件存储的最后一个数据位置
   * 如果该备份对象是远程节点对应的备份对象,则该值代表在leader节点上记录了该follow节点最近一次抓数据的起始位置,即如果follow节点从300开始抓去数据,则该数据表示300
   * 所有非leader节点的备份对象中,该值最小的一个,就是leader节点已经知道了所有同步对象中,最少也同步到哪个位置了,即highWatermarkMetadata在leader节点的值
   */
  @volatile private[this] var logEndOffsetMetadata: LogOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata
  // the time when log offset is updated,偏移量被更新的时间
  private[this] val logEndOffsetUpdateTimeMsValue = new AtomicLong(time.milliseconds)

  //该备份属于哪个topic-partition
  val topic = partition.topic
  val partitionId = partition.partitionId

  //true说明本地节点有log日志关于该partition,即该partition在本地机器有Replica数据
  def isLocal: Boolean = {
    log match {
      case Some(l) => true
      case None => false
    }
  }

  //设置文件的最后位置,注意本地备份文件是不需要设置该值的
  def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      throw new KafkaException("Should not set log end offset on partition [%s,%d]'s local replica %d".format(topic, partitionId, brokerId))
    } else {
      //设置文件位置以及更新时间
      logEndOffsetMetadata = newLogEndOffset
      logEndOffsetUpdateTimeMsValue.set(time.milliseconds)
      trace("Setting log end offset for replica %d for partition [%s,%d] to [%s]"
        .format(brokerId, topic, partitionId, logEndOffsetMetadata))
    }
  }

  //本地文件从log的信息中获取
  //非本地文件的话,则获取已经获取到的日志偏移量
  //设置该follow节点已经同步到哪个位置了
  //参见上面的方法是属于set方法,即logEndOffset_
  def logEndOffset =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

      //获取上次更新的时间
  def logEndOffsetUpdateTimeMs = logEndOffsetUpdateTimeMsValue.get()

  //为本地的日志文件设置commit数据位置
  //表示本地的该partiton文件已经同步到leader的哪个位置了
  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {
      highWatermarkMetadata = newHighWatermark
      trace("Setting high watermark for replica %d partition [%s,%d] on broker %d to [%s]"
        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
    } else {
      throw new KafkaException("Should not set high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  def highWatermark = highWatermarkMetadata

  //将本地的Replica对象转换为leader对象
  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      throw new KafkaException("Should not construct complete high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Replica]))
      return false
    val other = that.asInstanceOf[Replica]
    if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*brokerId + partition.hashCode()
  }


  //topic-partition-在broker上的备份
  override def toString(): String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
