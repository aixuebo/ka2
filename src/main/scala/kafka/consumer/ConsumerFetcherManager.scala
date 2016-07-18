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
import kafka.server.{BrokerAndInitialOffset, AbstractFetcherThread, AbstractFetcherManager}
import kafka.cluster.{Cluster, Broker}
import scala.collection.immutable
import scala.collection.Map
import collection.mutable.HashMap
import scala.collection.mutable
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.Utils.inLock
import kafka.utils.ZkUtils._
import kafka.utils.{ShutdownableThread, SystemTime}
import kafka.common.TopicAndPartition
import kafka.client.ClientUtils
import java.util.concurrent.atomic.AtomicInteger

/**
 *  Usage:
 *  Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 *  until shutdown() is called.
 *  为每一个消费者创建一个该对象,去抓取数据
 */
class ConsumerFetcherManager(private val consumerIdString: String,//标示消费者的唯一ID,格式config.groupId_消费者本地ip-时间戳-uuid
                             private val config: ConsumerConfig,
                             private val zkClient : ZkClient)//zookeeper连接器
        extends AbstractFetcherManager("ConsumerFetcherManager-%d".format(SystemTime.milliseconds),
                                       config.clientId, config.numConsumerFetchers) {//多少个线程去抓去数据

  private var partitionMap: immutable.Map[TopicAndPartition, PartitionTopicInfo] = null//为每一个topic-partition分配一个全局的元数据属性对象
  private var cluster: Cluster = null//集群中所有的节点集合
  private val noLeaderPartitionSet = new mutable.HashSet[TopicAndPartition]////目前尚未找到leader的topic-partition
  private val lock = new ReentrantLock
  private val cond = lock.newCondition()
  private var leaderFinderThread: ShutdownableThread = null //为每一个topic-partition查找leader节点的线程
  private val correlationId = new AtomicInteger(0)//请求ID

  //独立的一个线程
  private class LeaderFinderThread(name: String) extends ShutdownableThread(name) {
    // thread responsible for adding the fetcher to the right broker when leader is available 该线程负责找到leader节点,添加到抓去线程中去抓去该leader节点
    override def doWork() {//run方法会每次调用dowork方法
      val leaderForPartitionsMap = new HashMap[TopicAndPartition, Broker]//最终查找到的每一个topic-partition的leader节点映射关系
      lock.lock()
      try {
        while (noLeaderPartitionSet.isEmpty) {//说明此时没有找不到leader的topic-partition,则等待休息
          trace("No partition for leader election.")
          cond.await()
        }

        //代码执行到这里,说明已经有找不到leader的topic-partition了
        trace("Partitions without leader %s".format(noLeaderPartitionSet))//打印找不到的集合
        val brokers = getAllBrokersInCluster(zkClient) //获取/brokers/ids所有节点,并且过滤非有效的broker对象,获取当前集群中合法的broker的对象集合.并且已经排序后返回
        val topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.map(m => m.topic).toSet, //topic集合
                                                            brokers, //所有节点集合
                                                            config.clientId,
                                                            config.socketTimeoutMs,
                                                            correlationId.getAndIncrement).topicsMetadata//请求序号累加1
        if(logger.isDebugEnabled) topicsMetadata.foreach(topicMetadata => debug(topicMetadata.toString())) //对每一个topic元数据进行打印
        topicsMetadata.foreach { tmd => //循环每一个topic
          val topic = tmd.topic //获取topic名称
          tmd.partitionsMetadata.foreach { pmd => //循环该topic对应的poartition集合
            val topicAndPartition = TopicAndPartition(topic, pmd.partitionId)//每一个topic-partition
            if(pmd.leader.isDefined && noLeaderPartitionSet.contains(topicAndPartition)) {//存在leader,并且该客户端尚未找到leader
              val leaderBroker = pmd.leader.get //获取leader所在节点
              leaderForPartitionsMap.put(topicAndPartition, leaderBroker) //设置leader所在节点
              noLeaderPartitionSet -= topicAndPartition//移除,因为已经找到了该节点了
            }
          }
        }
      } catch {
        case t: Throwable => {
            if (!isRunning.get())
              throw t /* If this thread is stopped, propagate this exception to kill the thread. */
            else
              warn("Failed to find leader for %s".format(noLeaderPartitionSet), t)
          }
      } finally {
        lock.unlock()
      }

      try {
        //为topic-partition 到某个leader节点抓去从offset序号开始抓去数据
        addFetcherForPartitions(leaderForPartitionsMap.map{
          case (topicAndPartition, broker) =>
            topicAndPartition -> BrokerAndInitialOffset(broker, partitionMap(topicAndPartition).getFetchOffset())}
        )
      } catch {
        case t: Throwable => {
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          else {
            warn("Failed to add leader for partitions %s; will retry".format(leaderForPartitionsMap.keySet.mkString(",")), t)
            lock.lock()
            noLeaderPartitionSet ++= leaderForPartitionsMap.keySet//依然找不到leader
            lock.unlock()
          }
        }
      }

      //将闲置的抓取线程shutdown,即抓取线程没有任务的,则shutdown
      shutdownIdleFetcherThreads()
      Thread.sleep(config.refreshLeaderBackoffMs) //睡眠一阵子,每次刷新某个topic-partition的leader节点的时间间隔
    }
  }

  /**
   * 创建一个线程,去抓去broker的数据,该线程是线程池中的第fetcherId个线程
   */
  override def createFetcherThread(fetcherId: Int, sourceBroker: Broker): AbstractFetcherThread = {
    new ConsumerFetcherThread(
      "ConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id),
      config, sourceBroker, partitionMap, this)//创建一个消费者线程
  }

  /**
   * 要去抓去哪些topic-partition的信息
   */
  def startConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster) {

    //开启查找leader的线程
    leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread")
    leaderFinderThread.start()

    inLock(lock) {
      partitionMap = topicInfos.map(tpi => (TopicAndPartition(tpi.topic, tpi.partitionId), tpi)).toMap //将要抓去的topic-partition与元数据进行关联
      this.cluster = cluster
      noLeaderPartitionSet ++= topicInfos.map(tpi => TopicAndPartition(tpi.topic, tpi.partitionId))//目前尚未找到leader的topic-partition
      cond.signalAll()//激活锁,去查找这些topic-partition的leader节点去
    }
  }

  def stopConnections() {
    /*
     * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
     * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
     * these partitions.
     */
    info("Stopping leader finder thread")
    if (leaderFinderThread != null) {
      leaderFinderThread.shutdown()
      leaderFinderThread = null
    }

    info("Stopping all fetchers")
    closeAllFetchers()

    // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
    partitionMap = null
    noLeaderPartitionSet.clear()

    info("All connections stopped")
  }

  //向消费者抓取管理器,发送说参数这些topic-partition抓取失败了
  def addPartitionsWithError(partitionList: Iterable[TopicAndPartition]) {
    debug("adding partitions with error %s".format(partitionList))
    inLock(lock) {
      if (partitionMap != null) {
        noLeaderPartitionSet ++= partitionList //将这些topic-partition设置为没有leader
        cond.signalAll()//激活锁,去查找这些topic-partition的leader节点去
      }
    }
  }
}