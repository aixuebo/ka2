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

package kafka.log

import java.io.File
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import kafka.utils.{Logging, Pool}
import kafka.server.OffsetCheckpoint
import collection.mutable
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.Utils._
import java.util.concurrent.TimeUnit
import kafka.common.{LogCleaningAbortedException, TopicAndPartition}

//日志清理的状态
private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState //正在清理中
private[log] case object LogCleaningAborted extends LogCleaningState//清理终止了
private[log] case object LogCleaningPaused extends LogCleaningState//清理暂停了

/**
 *  Manage the state of each partition being cleaned.
 *  管理每一个被清理的partition的状态
 *  If a partition is to be cleaned, it enters the LogCleaningInProgress state.如果已经partition已经去清理了,则他的状态是清理中
 *  While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 *  the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 *  While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 *  requested to be resumed.
 *  如果一个partition已经是正在清理中了,则他依然能够接受请求去终止或者暂停清理,则此时partition要首先进入LogCleaningAborted终止状态,
 *  一旦清理任务真的被终止了,则状态改成LogCleaningPaused暂停状态.
 *  当partition在LogCleaningPaused状态的时候,我们不能被再次调度清理,除非清理请求恢复清理
 */
private[log] class LogCleanerManager(val logDirs: Array[File], val logs: Pool[TopicAndPartition, Log]) extends Logging with KafkaMetricsGroup {
  
  override val loggerName = classOf[LogCleaner].getName

  // package-private for testing
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"
  
  /* the offset checkpoints holding the last cleaned point for each log 
   * 每一个file对应一个 OffsetCheckpoint对象,即对应一个cleaner-offset-checkpoint文件
   * 即每一个文件夹下都记录了一个该文件夹下存储的topic-partition的偏移量
   **/
  private val checkpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, offsetCheckpointFile)))).toMap

  /* the set of logs currently being cleaned 
   * 每一个topic-partition对应的进度
   **/
  private val inProgress = mutable.HashMap[TopicAndPartition, LogCleaningState]()

  /* a global lock used to control all access to the in-progress set and the offset checkpoints */
  private val lock = new ReentrantLock
  
  /* for coordinating the pausing and the cleaning of a partition */
  private val pausedCleaningCond = lock.newCondition()
  
  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", new Gauge[Int] { def value = (100 * dirtiestLogCleanableRatio).toInt })

  /**
   * @return the position processed for all logs.
   * 从file文件中还原topic-partition对应的offset
   */
  def allCleanerCheckpoints(): Map[TopicAndPartition, Long] =
    checkpoints.values.flatMap(_.read()).toMap

   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
    * 获取脏数据
    */
  def grabFilthiestLog(): Option[LogToClean] = {
    inLock(lock) {
      val lastClean = allCleanerCheckpoints() //读取清理到什么偏移量了
      //转换成LogToClean集合
      val dirtyLogs = logs.filter(l => l._2.config.compact)          // skip any logs marked for delete rather than dedupe 找到prtition是compact的
                          .filterNot(l => inProgress.contains(l._1)) // skip any logs already in-progress 过滤剩余没有在清理中的partition
                          .map(l => LogToClean(l._1, l._2,           // create a LogToClean instance for each
                                               lastClean.getOrElse(l._1, l._2.logSegments.head.baseOffset)))
                          .filter(l => l.totalBytes > 0)             // skip any empty logs 跳过空的日志
      this.dirtiestLogCleanableRatio = if (!dirtyLogs.isEmpty) dirtyLogs.max.cleanableRatio else 0 //获取LogToClean中最大的cleanableRatio
      val cleanableLogs = dirtyLogs.filter(l => l.cleanableRatio > l.log.config.minCleanableRatio) // and must meet the minimum threshold for dirty byte ratio
      if(cleanableLogs.isEmpty) {
        None
      } else {//设置正在处理删除日志中
        val filthiest = cleanableLogs.max
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        Some(filthiest)
      }
    }
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
   */
  def abortCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      abortAndPauseCleaning(topicAndPartition)
      resumeCleaning(topicAndPartition)
      info("The cleaning for partition %s is aborted".format(topicAndPartition))
    }
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   *  1. If the partition is not in progress, mark it as paused.
   *  2. Otherwise, first mark the state of the partition as aborted.
   *  3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
   *     throws a LogCleaningAbortedException to stop the cleaning task.
   *  4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
   *  5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
   */
  def abortAndPauseCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
        case Some(state) =>
          state match {
            case LogCleaningInProgress =>
              inProgress.put(topicAndPartition, LogCleaningAborted)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be aborted and paused since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
      while (!isCleaningInState(topicAndPartition, LogCleaningPaused))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
      info("The cleaning for partition %s is aborted and paused".format(topicAndPartition))
    }
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is not paused."
                                          .format(topicAndPartition))
        case Some(state) =>
          state match {
            case LogCleaningPaused =>
              inProgress.remove(topicAndPartition)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
    }
    info("Compaction for partition %s is resumed".format(topicAndPartition))
  }

  /**
   *  Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
   *  true表示topicAndPartition在清理中,并且状态为expectedState
   */
  def isCleaningInState(topicAndPartition: TopicAndPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicAndPartition) match {
      case None => return false
      case Some(state) =>
        if (state == expectedState)
          return true
        else
          return false
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
   */
  def checkCleaningAborted(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      if (isCleaningInState(topicAndPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  //添加映射信息
  def updateCheckpoints(dataDir: File, update: Option[(TopicAndPartition,Long)]) {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      val existing = checkpoint.read().filterKeys(logs.keys) ++ update
      checkpoint.write(existing)
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicAndPartition: TopicAndPartition, dataDir: File, endOffset: Long) {
    inLock(lock) {
      inProgress(topicAndPartition) match {
        case LogCleaningInProgress =>
          updateCheckpoints(dataDir,Option(topicAndPartition, endOffset))
          inProgress.remove(topicAndPartition)
        case LogCleaningAborted =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case s =>
          throw new IllegalStateException("In-progress partition %s cannot be in %s state.".format(topicAndPartition, s))
      }
    }
  }
}
