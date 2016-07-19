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

package kafka.consumer

import org.I0Itec.zkclient.ZkClient
import kafka.common.TopicAndPartition
import kafka.utils.{Utils, ZkUtils, Logging}

/**
 * 消费者组向多个线程去分配partition的策略
 */
trait PartitionAssignor {

  /**
   * Assigns partitions to consumer instances in a group.
   * @return An assignment map of partition to consumer thread. This only includes assignments for threads that belong
   *         to the given assignment-context's consumer.
   *  分配,返回结果TopicAndPartition, ConsumerThreadId,表示哪些partition交给哪个线程去处理        
   */
  def assign(ctx: AssignmentContext): scala.collection.Map[TopicAndPartition, ConsumerThreadId]

}

object PartitionAssignor {
  def createInstance(assignmentStrategy: String) = assignmentStrategy match {
    case "roundrobin" => new RoundRobinAssignor()
    case _ => new RangeAssignor()
  }
}

/**
 * @group 消费组
 * @consumerId 消费者别名
 * @excludeInternalTopics true表示不能包含kafka内部的topic
 */
class AssignmentContext(group: String, val consumerId: String, excludeInternalTopics: Boolean, zkClient: ZkClient) {
  //获取该group的消费者ID 为每一个topic 已经分配的消费者线程集合
  val myTopicThreadIds: collection.Map[String, collection.Set[ConsumerThreadId]] = {
    ///consumers/${group}/ids/${consumerId} 内容{"pattern":"white_list、black_list、static之一","subscription":{"${topic}":2,"${topic}":2}  }
    val myTopicCount = TopicCount.constructTopicCount(group, consumerId, zkClient, excludeInternalTopics)
    myTopicCount.getConsumerThreadIdsPerTopic
  }

  /**
   * myTopicThreadIds.keySet.toSeq 返回该消费group-consumerId 对应的所有topic集合
   * 
   * 该方法读取/brokers/topics/${topic}的内容{partitions:{"1":[11,12,14],"2":[11,16,19]} } 含义是该topic中有两个partition,分别是1和2,每一个partition在哪些brokerId存储
   * 
   * 返回值:
   * 返回key是topic,value是该topic对应的partition集合.并且partition集合是按照顺序排好序l
   */
  val partitionsForTopic: collection.Map[String, Seq[Int]] =
    ZkUtils.getPartitionsForTopics(zkClient, myTopicThreadIds.keySet.toSeq)

    //返回属于该消费者组的topic与消费者线程集合映射,key是topic,value是该topic所有的消费者线程数组集合,eg 消费者1有3个线程,消费者2有4个线程,则value就是7个元素
  val consumersForTopic: collection.Map[String, List[ConsumerThreadId]] =
    ZkUtils.getConsumersPerTopic(zkClient, group, excludeInternalTopics)

  //返回该group下的所有消费者名称集合 ,返回/consumers/${group}/ids/的子节点集合
  val consumers: Seq[String] = ZkUtils.getConsumersInGroup(zkClient, group).sorted
}

/**
 * The round-robin partition assignor lays out all the available partitions and all the available consumer threads. It
 * then proceeds to do a round-robin assignment from partition to consumer thread. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumer threads.)
 *
 * (For simplicity of implementation) the assignor is allowed to assign a given topic-partition to any consumer instance
 * and thread-id within that instance. Therefore, round-robin assignment is allowed only if:
 * 为了简单的的实现,这个分配器被允许分配,将一个topic-partition分配给任意一个消费者实例的线程实例,因此有以下基本要求:
 * a) Every topic has the same number of streams within a consumer instance
 *   每一个toopic在每一个消费者里面有相同数量的流
 * b) The set of subscribed topics is identical for every consumer instance within the group.
 * 同一个group下,每一个消费者订阅的topic集合是完全相同的
 */
class RoundRobinAssignor() extends PartitionAssignor with Logging {

  //具体分配工作
  def assign(ctx: AssignmentContext) = {
    //返回结果TopicAndPartition, ConsumerThreadId,表示哪些partition交给哪个线程去处理        
    val partitionOwnershipDecision = collection.mutable.Map[TopicAndPartition, ConsumerThreadId]()

    // check conditions (a) and (b) 获取第一个topic和第一个topic对应的线程集合,用于校验是否是每一个topic的线程数量都相同
    /**
    ctx.consumersForTopic是Map[String, List[ConsumerThreadId]]
    因此ctx.consumersForTopic.head._1 就是topic字符串
      ctx.consumersForTopic.head._2 就是List[ConsumerThreadId]集合
     */
    val (headTopic, headThreadIdSet) = (ctx.consumersForTopic.head._1, ctx.consumersForTopic.head._2.toSet)
    
    //返回属于该消费者组的topic与消费者线程集合映射,key是topic,value是该topic所有的消费线程数组集合
    ctx.consumersForTopic.foreach { case (topic, threadIds) =>
      val threadIdSet = threadIds.toSet
      //校验每一个topic的线程数量都一样
      require(threadIdSet == headThreadIdSet,
              "Round-robin assignment is allowed only if all consumers in the group subscribe to the same topics, " +
              "AND if the stream counts across topics are identical for a given consumer instance.\n" +
              "Topic %s has the following available consumer streams: %s\n".format(topic, threadIdSet) +
              "Topic %s has the following available consumer streams: %s\n".format(headTopic, headThreadIdSet))
    }

    //排序消费者,按照消费者name-线程id排序的消费者集合迭代器
    val threadAssignor = Utils.circularIterator(headThreadIdSet.toSeq.sorted)

    info("Starting round-robin assignment with consumers " + ctx.consumers)
    /**
     * 计算所有的topic-partition,并且排序
     * 1.组成所有的topic-partition集合
     * 2.对集合排序
     */
    val allTopicPartitions = ctx.partitionsForTopic.flatMap { case(topic, partitions) =>
      info("Consumer %s rebalancing the following partitions for topic %s: %s"
           .format(ctx.consumerId, topic, partitions))
      partitions.map(partition => {
        TopicAndPartition(topic, partition)
      })
    }.toSeq.sortWith((topicPartition1, topicPartition2) => {
      /*
       * Randomize the order by taking the hashcode to reduce the likelihood of all partitions of a given topic ending
       * up on one consumer (if it has a high enough stream count).
       * 排序算法,先按照topic排序,在按照partition排序
       */
      topicPartition1.toString.hashCode < topicPartition2.toString.hashCode
    })

    //循环所有的TopicAndPartition对象
    allTopicPartitions.foreach(topicPartition => {
      val threadId = threadAssignor.next()//找到下一个消费者和对应的线程ID
      if (threadId.consumer == ctx.consumerId) //如果消费者是一样的,说明就是本消费者,则为本消费者分配数据
        partitionOwnershipDecision += (topicPartition -> threadId)//将这个topic-partition分配给该消费者处理
    })

    partitionOwnershipDecision
  }
}

/**
 * Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of
 * consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition. For example, suppose there are two consumers C1
 * and C2 with two streams each, and there are five available partitions (p0, p1, p2, p3, p4). So each consumer thread
 * will get at least one partition and the first consumer thread will get one extra partition. So the assignment will be:
 * p0 -> C1-0, p1 -> C1-0, p2 -> C1-1, p3 -> C2-0, p4 -> C2-1
 *
 * /**
 * 区间轮训算法
Sort PT (all partitions in topic T) 计算所有的topic-partition
Sort CG(all consumers in consumer group G) 计算该group下所有的消费者以及消费者对应的线程
Let i be the index position of Ci in CG and let N=size(PT)/size(CG) 找到每一个线程至少要消费多少个topic-partition,定义为N,该线程是在总线程池的第几个位置
Remove current entries owned by Ci from the partition owner registry 移除已经注册的该消费者消费哪些topic-partition
Assign partitions from iN to (i+1)N-1 to consumer Ci  i*N表示从哪个位置开始是我的线程要做的,一直到哪里是我要做的.有时候其实不需要+1
Add newly assigned partitions to the partition owner registry 重新添加该消费者消费哪些topic-partition
 */
 */
class RangeAssignor() extends PartitionAssignor with Logging {

  def assign(ctx: AssignmentContext) = {
    val partitionOwnershipDecision = collection.mutable.Map[TopicAndPartition, ConsumerThreadId]()

    for ((topic, consumerThreadIdSet) <- ctx.myTopicThreadIds) {
      val curConsumers = ctx.consumersForTopic(topic) //该topic一共多少个消费者线程在消费List[ConsumerThreadId]
      val curPartitions: Seq[Int] = ctx.partitionsForTopic(topic)//该topic一共多少个partition集合

      val nPartsPerConsumer = curPartitions.size / curConsumers.size //表示每一个消费者要消费多少个该topic的partition
      val nConsumersWithExtraPart = curPartitions.size % curConsumers.size //

      info("Consumer " + ctx.consumerId + " rebalancing the following partitions: " + curPartitions +
        " for topic " + topic + " with consumers: " + curConsumers)

      /**
       * 假设10个partition 6个线程,首先一个线程分配一个partition,然后剩余4个则分配给前四个线程
       */
      for (consumerThreadId <- consumerThreadIdSet) {
        val myConsumerPosition = curConsumers.indexOf(consumerThreadId)
        assert(myConsumerPosition >= 0)
        val startPart = nPartsPerConsumer * myConsumerPosition + myConsumerPosition.min(nConsumersWithExtraPart)//从哪里开始消费,前面表示计算平均值应该在哪个位置,因为有些线程需要消费多个partition,因此后面做这个的
        val nParts = nPartsPerConsumer + (if (myConsumerPosition + 1 > nConsumersWithExtraPart) 0 else 1) //判断是否要多消费1个partition,nPartsPerConsumer表示本来就要消费多少个,另外配上是否要另外多加一个

        /**
         *   Range-partition the sorted partitions to consumers for better locality.
         *  The first few consumers pick up an extra partition, if any.
         */
        if (nParts <= 0)
          warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic)
        else {
          for (i <- startPart until startPart + nParts) {
            val partition = curPartitions(i)
            info(consumerThreadId + " attempting to claim partition " + partition)
            // record the partition ownership decision
            partitionOwnershipDecision += (TopicAndPartition(topic, partition) -> consumerThreadId)
          }
        }
      }
    }

    partitionOwnershipDecision
  }
}
