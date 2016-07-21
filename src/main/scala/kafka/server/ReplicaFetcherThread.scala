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

import kafka.admin.AdminUtils
import kafka.cluster.Broker
import kafka.log.LogConfig
import kafka.message.ByteBufferMessageSet
import kafka.api.{OffsetRequest, FetchResponsePartitionData}
import kafka.common.{KafkaStorageException, TopicAndPartition}

//向partition的leader抓取数据的线程
class ReplicaFetcherThread(name:String,
                           sourceBroker: Broker,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                socketTimeout = brokerConfig.replicaSocketTimeoutMs,
                                socketBufferSize = brokerConfig.replicaSocketReceiveBufferBytes,
                                fetchSize = brokerConfig.replicaFetchMaxBytes,
                                fetcherBrokerId = brokerConfig.brokerId,//本地消费者节点
                                maxWait = brokerConfig.replicaFetchWaitMaxMs,
                                minBytes = brokerConfig.replicaFetchMinBytes,
                                isInterruptible = false) {

  // process fetched data处理抓取的数据,抓取topic-partition数据,从fetchOffset位置开始抓取,抓取的内容在FetchResponsePartitionData中
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: FetchResponsePartitionData) {
    try {
      val topic = topicAndPartition.topic
      val partitionId = topicAndPartition.partition
      //获取本地节点的存储partition的对象
      val replica = replicaMgr.getReplica(topic, partitionId).get
      val messageSet = partitionData.messages.asInstanceOf[ByteBufferMessageSet]//抓去回来的message集合信息

      if (fetchOffset != replica.logEndOffset.messageOffset)//抓去的序号和已经存储的下一个序号要相匹配,不匹配则抛异常
        throw new RuntimeException("Offset mismatch: fetched offset = %d, log end offset = %d.".format(fetchOffset, replica.logEndOffset.messageOffset))
      
      //输出某个子节点 从xxx位置开始追加topic-partition的数据,追加xx个信息,leader节点是xxx
      trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
            .format(replica.brokerId, replica.logEndOffset.messageOffset, topicAndPartition, messageSet.sizeInBytes, partitionData.hw))
       
      //向本地文件写入信息,并且记录日志,xxx节点接收到xxx字节信息后,已经到了日志xxx位置,接收的日志是某个topic-partition的日志
      replica.log.get.append(messageSet, assignOffsets = false)
      trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
            .format(replica.brokerId, replica.logEndOffset.messageOffset, messageSet.sizeInBytes, topicAndPartition))
            
      //获取文件的最后一个位置
      val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.hw)
      // for the follower replica, we do not need to keep
      // its segment base offset the physical position,
      // these values will be computed upon making the leader
      //做为跟随节点,因此我们不需要保持物理文件的基础位置,这个值仅仅需要被leader计算即可,因此我们只需要记录了followerHighWatermark值即可
      replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)
      trace("Follower %d set replica high watermark for partition [%s,%d] to %s"
            .format(replica.brokerId, topic, partitionId, followerHighWatermark))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error while replicating data.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    val replica = replicaMgr.getReplica(topicAndPartition.topic, topicAndPartition.partition).get

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val leaderEndOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.LatestTime, brokerConfig.brokerId)
    if (leaderEndOffset < replica.logEndOffset.messageOffset) {
      // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.
      // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
      // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.
      if (!LogConfig.fromProps(brokerConfig.props.props, AdminUtils.fetchTopicConfig(replicaMgr.zkClient,
        topicAndPartition.topic)).uncleanLeaderElectionEnable) {
        // Log a fatal error and shutdown the broker to ensure that data loss does not unexpectedly occur.
        fatal("Halting because log truncation is not allowed for topic %s,".format(topicAndPartition.topic) +
          " Current leader %d's latest offset %d is less than replica %d's latest offset %d"
          .format(sourceBroker.id, leaderEndOffset, brokerConfig.brokerId, replica.logEndOffset.messageOffset))
        Runtime.getRuntime.halt(1)
      }

      replicaMgr.logManager.truncateTo(Map(topicAndPartition -> leaderEndOffset))
      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderEndOffset))
      leaderEndOffset
    } else {
      /**
       * The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       *
       * Roll out a new log at the follower with the start offset equal to the current leader's start offset and continue fetching.
       */
      val leaderStartOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.EarliestTime, brokerConfig.brokerId)
      replicaMgr.logManager.truncateFullyAndStartAt(topicAndPartition, leaderStartOffset)
      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderStartOffset))
      leaderStartOffset
    }
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    // no handler needed since the controller will make the changes accordingly
  }
}
