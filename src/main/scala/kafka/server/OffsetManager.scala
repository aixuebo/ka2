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

import org.apache.kafka.common.protocol.types.{Struct, Schema, Field}
import org.apache.kafka.common.protocol.types.Type.STRING
import org.apache.kafka.common.protocol.types.Type.INT32
import org.apache.kafka.common.protocol.types.Type.INT64

import kafka.utils._
import kafka.common._
import kafka.log.{FileMessageSet, LogConfig}
import kafka.message._
import kafka.metrics.KafkaMetricsGroup
import kafka.common.TopicAndPartition
import kafka.tools.MessageFormatter

import scala.Some
import scala.collection._
import java.io.PrintStream
import java.util.concurrent.atomic.AtomicBoolean
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import org.I0Itec.zkclient.ZkClient


/**
 * Configuration settings for in-built offset management
 * @param maxMetadataSize The maximum allowed metadata for any offset commit.
 * @param loadBufferSize Batch size for reading from the offsets segments when loading offsets into the cache.
 * @param offsetsRetentionMs Offsets older than this retention period will be discarded.缓存offset时间
 * @param offsetsRetentionCheckIntervalMs Frequency at which to check for stale offsets. 一定周期执行一次compact方法
 * @param offsetsTopicNumPartitions The number of partitions for the offset commit topic (should not change after deployment).topic为__consumer_offsets的partition数量,按照group进行拆分partition
 * @param offsetsTopicSegmentBytes The offsets topic segment bytes should be kept relatively small to facilitate faster
 *                                 log compaction and faster offset loads
 * @param offsetsTopicReplicationFactor The replication factor for the offset commit topic (set higher to ensure availability).
 * @param offsetsTopicCompressionCodec Compression codec for the offsets topic - compression should be turned on in
 *                                     order to achieve "atomic" commits.
 * @param offsetCommitTimeoutMs The offset commit will be delayed until all replicas for the offsets topic receive the
 *                              commit or this timeout is reached. (Similar to the producer request timeout.)
 * @param offsetCommitRequiredAcks The required acks before the commit can be accepted. In general, the default (-1) 
 *                                 should not be overridden.
 * 用于存储group-topic-partition的offset信息的配置 ,即每一个消费组消费每一个topic-partition到哪个序号了
 * 只有存储该topic的log节点上或者备份节点上有该对象 ,但是每一个节点都会产生一个该对象,只是可能该对象没有意义
 */
case class OffsetManagerConfig(maxMetadataSize: Int = OffsetManagerConfig.DefaultMaxMetadataSize,//offset最多允许多少个字符存储描述信息,超过该限制的则不要该记录
                               loadBufferSize: Int = OffsetManagerConfig.DefaultLoadBufferSize,//加载offset的topic-partition文件需要的内存缓冲区大小
                               offsetsRetentionMs: Long = 24*60*60000L,//默认24小时,缓存offset时间
                               offsetsRetentionCheckIntervalMs: Long = OffsetManagerConfig.DefaultOffsetsRetentionCheckIntervalMs,//一定周期执行一次compact方法
                               offsetsTopicNumPartitions: Int = OffsetManagerConfig.DefaultOffsetsTopicNumPartitions,//按照group进行拆分partition
                               offsetsTopicSegmentBytes: Int = OffsetManagerConfig.DefaultOffsetsTopicSegmentBytes,
                               offsetsTopicReplicationFactor: Short = OffsetManagerConfig.DefaultOffsetsTopicReplicationFactor,//partition的备份数
                               offsetsTopicCompressionCodec: CompressionCodec = OffsetManagerConfig.DefaultOffsetsTopicCompressionCodec,//向offset对应的topic中写入的信息需要什么压缩方式
                               offsetCommitTimeoutMs: Int = OffsetManagerConfig.DefaultOffsetCommitTimeoutMs,//作为生产者提交该topic-partition的时候,要求回执信息的超时时间
                               offsetCommitRequiredAcks: Short = OffsetManagerConfig.DefaultOffsetCommitRequiredAcks) //作为生产者提交该topic-partition的时候,是否需要处理回执信息

object OffsetManagerConfig {
  val DefaultMaxMetadataSize = 4096
  val DefaultLoadBufferSize = 5*1024*1024
  val DefaultOffsetsRetentionCheckIntervalMs = 600000L
  val DefaultOffsetsTopicNumPartitions = 50
  val DefaultOffsetsTopicSegmentBytes = 100*1024*1024
  val DefaultOffsetsTopicReplicationFactor = 3.toShort
  val DefaultOffsetsTopicCompressionCodec = NoCompressionCodec
  val DefaultOffsetCommitTimeoutMs = 5000
  val DefaultOffsetCommitRequiredAcks = (-1).toShort
}

/**
 * 用于在本地服务器记录topic-partition的偏移量信息
 * 存储这些信息也是在kafka中存储的,topic固定为__consumer_offsets,partition数量为OffsetManagerConfig.offsetsTopicNumPartitions
 */
class OffsetManager(val config: OffsetManagerConfig,
                    replicaManager: ReplicaManager,
                    zkClient: ZkClient,
                    scheduler: Scheduler) extends Logging with KafkaMetricsGroup {

  /* offsets and metadata cache
   * 做一个缓存,key表示group-topic-partition,value表示该partition对应的偏移量以及描述信息 
   **/
  private val offsetsCache = new Pool[GroupTopicPartition, OffsetAndMetadata]
  private val followerTransitionLock = new Object//锁对象

  //加载topic为__consumer_offsets的第offsetsPartition个partition,是异步的方式读取
  //该值存储的是第几个offsetsPartition正在被异步加载中,加载完成后就会从该set中清除
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  private val shuttingDown = new AtomicBoolean(false)

  //一定周期执行一次compact方法
  scheduler.schedule(name = "offsets-cache-compactor",
                     fun = compact,
                     period = config.offsetsRetentionCheckIntervalMs,
                     unit = TimeUnit.MILLISECONDS)

  newGauge("NumOffsets",
    new Gauge[Int] {
      def value = offsetsCache.size
    }
  )

  newGauge("NumGroups",
    new Gauge[Int] {
      def value = offsetsCache.keys.map(_.group).toSet.size
    }
  )

  //一定周期执行一次compact紧凑方法,就是定期删除一些topic的offset的记录信息
  private def compact() {
    debug("Compacting offsets cache.")
    val startMs = SystemTime.milliseconds

    //过滤查找超过config.offsetsRetentionMs时间的数据,返回值Iterable[(GroupTopicPartition, OffsetAndMetadata)]
    val staleOffsets = offsetsCache.filter(startMs - _._2.timestamp > config.offsetsRetentionMs)

    //打印日志,发现多少个超过config.offsetsRetentionMs时间的数据
    debug("Found %d stale offsets (older than %d ms).".format(staleOffsets.size, config.offsetsRetentionMs))

    // delete the stale offsets from the table and generate tombstone messages to remove them from the log
    /**
     * 1.移除超过config.offsetsRetentionMs时间的数据
     * 
     * 返回值 Map[Int, Iterable[(Int, Message)]] key是partition,value是 offsetsPartition, new Message(bytes = null, key = commitKey) 组成的元组
     * 说明offsetsPartition是该group所在的分区,value是group-topic-partition组成的字节数组
     */
    val tombstonesForPartition = staleOffsets.map { case(groupTopicAndPartition, offsetAndMetadata) =>
      val offsetsPartition = partitionFor(groupTopicAndPartition.group) //group在哪个partition中存储
      //打印日志,移除腐朽的偏移量和描述信息 for group-topic-partition:offset-message-时间戳
      trace("Removing stale offset and metadata for %s: %s".format(groupTopicAndPartition, offsetAndMetadata))
      //真正的去移除
      offsetsCache.remove(groupTopicAndPartition)

      //val commitKey: Array[Byte],用字节数组组装成key
      val commitKey = OffsetManager.offsetCommitKey(groupTopicAndPartition.group,
        groupTopicAndPartition.topicPartition.topic, groupTopicAndPartition.topicPartition.partition)//组装成key

      (offsetsPartition, new Message(bytes = null, key = commitKey))
    }.groupBy{ case (partition, tombstone) => partition }

    // Append the tombstone messages to the offset partitions. It is okay if the replicas don't receive these (say,
    // if we crash or leaders move) since the new leaders will get rid of stale offsets during their own purge cycles.
    //返回的就是删除了多少个group-topic-partition组合存储的偏移量
    val numRemoved = tombstonesForPartition.flatMap { case(offsetsPartition, tombstones) =>
      val partitionOpt = replicaManager.getPartition(OffsetManager.OffsetsTopicName, offsetsPartition) //获取topic为__consumer_offsets的,第offsetsPartition个partition
      partitionOpt.map { partition =>
        val appendPartition = TopicAndPartition(OffsetManager.OffsetsTopicName, offsetsPartition) //找到topic-partition
        val messages = tombstones.map(_._2).toSeq //在该partition中要操作的key集合,key由group-topic-partition组成

        //日志,标记多少组group-topic-partition的偏移量 在xxxpartition中存储的内容要被删除掉
        trace("Marked %d offsets in %s for deletion.".format(messages.size, appendPartition))

        try {
          //向topic:__consumer_offsets对应的一个partition中写入信息,即写入group-topic-partition信息
          partition.appendMessagesToLeader(new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, messages:_*))//因为没有写入value,即没有写入对应的消费的序号,因此等下次加载的时候,就会被认为是删除的,不会被加入到内存中
          tombstones.size
        }
        catch {
          case t: Throwable =>
            error("Failed to mark %d stale offsets for deletion in %s.".format(messages.size, appendPartition), t)
            // ignore and continue
            0
        }
      }
    }.sum

    //移除多少个,花费多少时间
    debug("Removed %d stale offsets in %d milliseconds.".format(numRemoved, SystemTime.milliseconds - startMs))
  }

  def offsetsTopicConfig: Properties = {
    val props = new Properties
    props.put(LogConfig.SegmentBytesProp, config.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CleanupPolicyProp, "compact")
    props
  }

  //返回该group在哪个partition中存储
  def partitionFor(group: String): Int = Utils.abs(group.hashCode) % config.offsetsTopicNumPartitions

  /**
   * Fetch the current offset for the given group/topic/partition from the underlying offsets storage.
   *
   * @param key The requested group-topic-partition
   * @return If the key is present, return the offset and metadata; otherwise return None
   * 获取topic-partition对应的偏移量和描述信息
   */
  private def getOffset(key: GroupTopicPartition) = {
    val offsetAndMetadata = offsetsCache.get(key)
    if (offsetAndMetadata == null)
      OffsetMetadataAndError.NoOffset //说明没有topic-partition对应的offset数据
    else
      OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError)
  }

  /**
   * Put the (already committed) offset for the given group/topic/partition into the cache.
   *
   * @param key The group-topic-partition
   * @param offsetAndMetadata The offset/metadata to be stored
   * 为一个topic-partition添加对应的offset映射
   */
  private def putOffset(key: GroupTopicPartition, offsetAndMetadata: OffsetAndMetadata) {
    offsetsCache.put(key, offsetAndMetadata)
  }

  //批量存储,一个group可以操作一组topic-partition
  def putOffsets(group: String, offsets: Map[TopicAndPartition, OffsetAndMetadata]) {
    // this method is called _after_ the offsets have been durably appended to the commit log, so there is no need to
    // check for current leadership as we do for the offset fetch
    trace("Putting offsets %s for group %s in offsets partition %d.".format(offsets, group, partitionFor(group)))
    offsets.foreach { case (topicAndPartition, offsetAndMetadata) =>
      putOffset(GroupTopicPartition(group, topicAndPartition), offsetAndMetadata)
    }
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   * 获取某个group下,若干TopicAndPartition对应的每一个TopicAndPartition和偏移量信息
   */
  def getOffsets(group: String, topicPartitions: Seq[TopicAndPartition]): Map[TopicAndPartition, OffsetMetadataAndError] = {
    trace("Getting offsets %s for group %s.".format(topicPartitions, group))

    //计算group所在partition分区
    val offsetsPartition = partitionFor(group)

    /**
     * followerTransitionLock protects against fetching from an empty/cleared offset cache (i.e., cleared due to a
     * leader->follower transition). i.e., even if leader-is-local is true a follower transition can occur right after
     * the check and clear the cache. i.e., we would read from the empty cache and incorrectly return NoOffset.
     */
    followerTransitionLock synchronized {
      if (leaderIsLocal(offsetsPartition)) {//说明该partition是本地文件
        if (loadingPartitions synchronized loadingPartitions.contains(offsetsPartition)) {//说明该partition正在加载中
          debug("Cannot fetch offsets for group %s due to ongoing offset load.".format(group)) //不能抓去该partition,因为该partition正在不间断的加载中
          topicPartitions.map { topicAndPartition =>
            val groupTopicPartition = GroupTopicPartition(group, topicAndPartition)
            (groupTopicPartition.topicPartition, OffsetMetadataAndError.OffsetsLoading) //说明该partition正在加载中,还没有加载完成
          }.toMap
        } else {
          if (topicPartitions.size == 0) { //说明没有要查询的topic-partition
           // Return offsets for all partitions owned by this consumer group. (this only applies to consumers that commit offsets to Kafka.)
            offsetsCache.filter(_._1.group == group).map { case(groupTopicPartition, offsetAndMetadata) =>
              (groupTopicPartition.topicPartition, OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError))
            }.toMap
          } else {
            topicPartitions.map { topicAndPartition =>
              val groupTopicPartition = GroupTopicPartition(group, topicAndPartition)
              (groupTopicPartition.topicPartition, getOffset(groupTopicPartition))
            }.toMap //返回该topic-partition对应的偏移量
          }
        }
      } else { //说明该partition不是本地的文件
        debug("Could not fetch offsets for group %s (not offset coordinator).".format(group))
        topicPartitions.map { topicAndPartition =>
          val groupTopicPartition = GroupTopicPartition(group, topicAndPartition)
          (groupTopicPartition.topicPartition, OffsetMetadataAndError.NotOffsetManagerForGroup) //说明本地磁盘上没有该partition对应的文件
        }.toMap
      }
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   * 异步的方式从日志中加载第offsetsPartition个topic-partition到内存中,注意topic为__consumer_offsets
   */
  def loadOffsetsFromLog(offsetsPartition: Int) {

    //加载topic:__consumer_offsets的哪个partition
    val topicPartition = TopicAndPartition(OffsetManager.OffsetsTopicName, offsetsPartition)

    loadingPartitions synchronized {
      if (loadingPartitions.contains(offsetsPartition)) {//说明该partition正在被加载的过程中
        info("Offset load from %s already in progress.".format(topicPartition))
      } else {//说明该partition没有被加载,因此添加到集合中,表示该partition正在加载中
        loadingPartitions.add(offsetsPartition)
        scheduler.schedule(topicPartition.toString, loadOffsets)//去执行loadOffsets函数
      }
    }

    //加载topic为__consumer_offsets的第offsetsPartition个partition,是异步的方式读取
    def loadOffsets() {
      //开始加载该partition
      info("Loading offsets from " + topicPartition)

      val startMs = SystemTime.milliseconds
      try {
        replicaManager.logManager.getLog(topicPartition) match {//读取制定topic-partition对应的LOG文件
          case Some(log) =>
            //开始读取该partition数据
            var currOffset = log.logSegments.head.baseOffset//获取第1个segment文件的第一个偏移量,也就是目前该partition存储的最小的序号
            val buffer = ByteBuffer.allocate(config.loadBufferSize) //创建缓冲区
            // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
            while (currOffset < getHighWatermark(offsetsPartition) && !shuttingDown.get()) {//不断循环读取数据
              buffer.clear() //重置buffer缓冲区
              val messages = log.read(currOffset, config.loadBufferSize).messageSet.asInstanceOf[FileMessageSet]
              messages.readInto(buffer, 0)
              val messageSet = new ByteBufferMessageSet(buffer)
              messageSet.foreach { msgAndOffset =>
                require(msgAndOffset.message.key != null, "Offset entry key should not be null")
                val key = OffsetManager.readMessageKey(msgAndOffset.message.key) //获取key对象GroupTopicPartition
                if (msgAndOffset.message.payload == null) {//说明没有对应的value
                  if (offsetsCache.remove(key) != null) //从内存中删除
                    trace("Removed offset for %s due to tombstone entry.".format(key))
                  else
                    trace("Ignoring redundant tombstone for %s.".format(key))
                } else {
                  val value = OffsetManager.readMessageValue(msgAndOffset.message.payload) //获取对应的value值
                  putOffset(key, value) //存储key--value键值对
                  trace("Loaded offset %s for %s.".format(value, key))
                }
                currOffset = msgAndOffset.nextOffset
              }
            }//结束不断循环读取数据

            if (!shuttingDown.get())
              info("Finished loading offsets from %s in %d milliseconds."
                   .format(topicPartition, SystemTime.milliseconds - startMs))//完成加载第i个partition对应的offset文件,花费了多久时间
          case None =>
            warn("No log found for " + topicPartition) //没有找到该partition文件
        }
      }
      catch {
        case t: Throwable =>
          error("Error in loading offsets from " + topicPartition, t)
      }
      finally {
        loadingPartitions synchronized loadingPartitions.remove(offsetsPartition) //该partition已经加载完成,因此从正在处理的队列中删除
      }
    }
  }

  //获取该topic:__consumer_offsets 对应的partition已经写入到了哪个偏移量了
  private def getHighWatermark(partitionId: Int): Long = {
    val partitionOpt = replicaManager.getPartition(OffsetManager.OffsetsTopicName, partitionId)

    //是本地的文件,则会返回当前下一个message的序号,该序号就是全局的序号,不是本地文件,则返回-1
    val hw = partitionOpt.map { partition =>
      partition.leaderReplicaIfLocal().map(_.highWatermark.messageOffset).getOrElse(-1L)
    }.getOrElse(-1L)

    hw
  }

  //leader在本地返回true
  private def leaderIsLocal(partition: Int) = { getHighWatermark(partition) != -1L }

  /**
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   * 删除制定offset的partition下所有的数据
   */
  def clearOffsetsInPartition(offsetsPartition: Int) {
    debug("Deleting offset entries belonging to [%s,%d].".format(OffsetManager.OffsetsTopicName, offsetsPartition))

    followerTransitionLock synchronized {
      offsetsCache.keys.foreach { key =>
        if (partitionFor(key.group) == offsetsPartition) {//如果该group对应的是参数的partition,则删除这些数据
          offsetsCache.remove(key)
        }
      }
    }
  }

  def shutdown() {
    shuttingDown.set(true)
  }

}

object OffsetManager {

  val OffsetsTopicName = "__consumer_offsets"

  //包含key和value的定义文件
  private case class KeyAndValueSchemas(keySchema: Schema, valueSchema: Schema)

  private val CURRENT_OFFSET_SCHEMA_VERSION = 0.toShort //当前版本号

  //key的定义,三个属性 String的group,String的topic,int的partition
  private val OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(new Field("group", STRING),
                                                       new Field("topic", STRING),
                                                       new Field("partition", INT32))
  private val KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("group")
  private val KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("topic")
  private val KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("partition")

  //value的定义,三个属性 long的offset,String的metadata,Long的timestamp
  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
                                                         new Field("metadata", STRING, "Associated metadata.", ""),
                                                         new Field("timestamp", INT64))
  private val VALUE_OFFSET_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val VALUE_METADATA_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val VALUE_TIMESTAMP_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  // map of versions to schemas 表示每一个版本对应一个KeyAndValueSchemas对象,即KeyAndValueSchemas对象中包含了该版本需要哪些字段
  //因为目前就一个版本0,因此该map中就一个键值对
  private val OFFSET_SCHEMAS = Map(0 -> KeyAndValueSchemas(OFFSET_COMMIT_KEY_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V0))

  //获取当前版本号对应的KeyAndValueSchemas对象
  private val CURRENT_SCHEMA = schemaFor(CURRENT_OFFSET_SCHEMA_VERSION)

  //通过版本号 返回KeyAndValueSchemas对象,包含key和value的定义文件
  private def schemaFor(version: Int) = {
    val schemaOpt = OFFSET_SCHEMAS.get(version) //查找该版本号对应的KeyAndValueSchemas对象
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version) //说明没有找到该版本号对应的数据
    }
  }

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   *
   * @return key for offset commit message
   * 用Schema定义字节数组,存储内容:
   * versionId、group、topic、partition
   * 最终字节数组中存储的是2个字节存储version,剩下字节存储group、topic、partition
   */
  def offsetCommitKey(group: String, topic: String, partition: Int, versionId: Short = 0): Array[Byte] = {
    val key = new Struct(CURRENT_SCHEMA.keySchema) //设置key
    key.set(KEY_GROUP_FIELD, group)
    key.set(KEY_TOPIC_FIELD, topic)
    key.set(KEY_PARTITION_FIELD, partition)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array() //最终字节数组中存储的是2个字节存储version,剩下字节存储group、topic、partition
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   *
   * @param offsetAndMetadata consumer's current offset and metadata
   * @return payload for offset commit message
   * 用Schema定义字节数组,存储内容:
   * versionId、offset、metadata、timestamp
   */
  def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata): Array[Byte] = {
    val value = new Struct(CURRENT_SCHEMA.valueSchema) //设置VALUE
    value.set(VALUE_OFFSET_FIELD, offsetAndMetadata.offset)
    value.set(VALUE_METADATA_FIELD, offsetAndMetadata.metadata)
    value.set(VALUE_TIMESTAMP_FIELD, offsetAndMetadata.timestamp)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array() //最终字节数组中存储的是2个字节存储version,剩下字节存储offset、metadata、timestamp
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an GroupTopicPartition object
   * 从ByteBuffer流中根据version找到对应的schema,从而可以从流中获取group、topic、partition
   * 
   * 从ByteBuffer中获取key对象GroupTopicPartition
   */
  def readMessageKey(buffer: ByteBuffer): GroupTopicPartition = {
    //从ByteBuffer流中根据version找到对应的schema
    val version = buffer.getShort()
    val keySchema = schemaFor(version).keySchema
    val key = keySchema.read(buffer).asInstanceOf[Struct]

    //从而可以从流中获取group、topic、partition
    val group = key.get(KEY_GROUP_FIELD).asInstanceOf[String]
    val topic = key.get(KEY_TOPIC_FIELD).asInstanceOf[String]
    val partition = key.get(KEY_PARTITION_FIELD).asInstanceOf[Int]

    //组装成返回值
    GroupTopicPartition(group, TopicAndPartition(topic, partition))
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   * 
   * 从ByteBuffer流中根据version找到对应的schema,从而可以从流中获取offset、metadata、timestamp
   * 
   * 从ByteBuffer中获取key对象GroupTopicPartition
   */
  def readMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if(buffer == null) { // tombstone
      null
    } else {
      
      //从ByteBuffer流中根据version找到对应的schema
      val version = buffer.getShort()
      val valueSchema = schemaFor(version).valueSchema
      val value = valueSchema.read(buffer).asInstanceOf[Struct]
      //从而可以从流中获取offset、metadata、timestamp
      val offset = value.get(VALUE_OFFSET_FIELD).asInstanceOf[Long]
      val metadata = value.get(VALUE_METADATA_FIELD).asInstanceOf[String]
      val timestamp = value.get(VALUE_TIMESTAMP_FIELD).asInstanceOf[Long]
      //组装成返回值
      OffsetAndMetadata(offset, metadata, timestamp)
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.server.OffsetManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  //为用户提供一个格式化工具,比如消费者控制台
  class OffsetsMessageFormatter extends MessageFormatter {
    
      //从key和value字节数组中,反序列化成TopicAndPartition和OffsetAndMetadata对象,然后输出到PrintStream输出流中
    //注意:两个对象在输出流中的分隔符是::
    def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {
      val formattedKey = if (key == null) "NULL" else OffsetManager.readMessageKey(ByteBuffer.wrap(key)).toString //打印key的日志信息
      val formattedValue = if (value == null) "NULL" else OffsetManager.readMessageValue(ByteBuffer.wrap(value)).toString //打印value的日志信息
      output.write(formattedKey.getBytes)
      output.write("::".getBytes)//两个对象的分隔符
      output.write(formattedValue.getBytes)
      output.write("\n".getBytes)
    }
  }

}

/**
 * 对应一个组group--topic-partition映射信息
 */
case class GroupTopicPartition(group: String, topicPartition: TopicAndPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicAndPartition(topic, partition))

  override def toString =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)

}
