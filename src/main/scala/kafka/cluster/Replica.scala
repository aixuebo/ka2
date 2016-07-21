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
              val partition: Partition,
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,//该partition在本地的最高offset
              val log: Option[Log] = None) extends Logging {//该partition对应的本地log文件
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  //一个高水印位置,在非leader备份中仅仅
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  //日志最后的偏移量位置,保存在所有的备份数据中
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  //对于本地的备份,该值表示日志的最后一个位置,对于远程的备份,该值表示抓取到了哪里,用于follower节点
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
