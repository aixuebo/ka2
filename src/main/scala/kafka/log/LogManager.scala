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

import java.io._
import java.util.concurrent.TimeUnit
import kafka.utils._
import scala.collection._
import kafka.common.{TopicAndPartition, KafkaException}
import kafka.server.{RecoveringFromUncleanShutdown, BrokerState, OffsetCheckpoint}
import java.util.concurrent.{Executors, ExecutorService, ExecutionException, Future}

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
 * 
 * 
 * 存储格式:
 * dataDir/topic-partition/存储该log的信息文件
 */
@threadsafe
class LogManager(val logDirs: Array[File],//目录集合,可以处理目录存储log日志
                 val topicConfigs: Map[String, LogConfig],
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,//线程数,多线程处理logDirs目录集合中的每一个目录,即每一个目录都对应一个该线程数量的线程池
                 val flushCheckMs: Long,
                 val flushCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 private val time: Time) extends Logging {
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LockFile = ".lock"
  val InitialTaskDelayMs = 30*1000
  
  
  private val logCreationOrDeletionLock = new Object//锁对象,用于创建或者删除一个topic-partition对应的LOG文件时,使用该锁
  
  //每一个topic-partition对应一个该LOG对象
  private val logs = new Pool[TopicAndPartition, Log]()

  //校验参数必须都是目录、并且有可读权限、并且如果目录不存在,则创建该目录
  createAndValidateLogDirs(logDirs)
  //为每一个目录下创建一个.lock文件,如果出现异常,则抛出
  private val dirLocks = lockLogDirs(logDirs)
  
  //每一个目录,对应一个recovery-point-offset-checkpoint文件,用于标示该目录下topic-partition同步到哪些offset了
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap
  
  loadLogs()

  // public, so we can access this from kafka.admin.DeleteTopicTest
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, logDirs, logs, time = time)
    else
      null
  
  /**
   * Create and check validity of the given directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list确保目录集合中所有的目录都是唯一的,没有重复的目录
   * <li> Create each directory if it doesn't exist 每一个目录如果不存在,则要创建该目录
   * <li> Check that each path is a readable directory 检查每一个必须是目录,并且必须是可以读操作的
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File]) {
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)//确保目录集合中所有的目录都是唯一的,没有重复的目录
      throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))
    for(dir <- dirs) {
      if(!dir.exists) {
        info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
        val created = dir.mkdirs()
        if(!created)
          throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
      }
      if(!dir.isDirectory || !dir.canRead)
        throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
    }
  }
  
  /**
   * Lock all the given directories
   * 为每一个目录下创建一个.lock文件,如果出现异常,则抛出
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.map { dir =>
      val lock = new FileLock(new File(dir, LockFile))
      if(!lock.tryLock())
        throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath + 
                               ". A Kafka instance in another process or thread is using this directory.")
      lock
    }
  }
  
  /**
   * Recover and load all logs in the given data directories
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    for (dir <- this.logDirs) {
      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)

      if (cleanShutdownFile.exists) {
        debug(
          "Found clean shutdown file. " +
          "Skipping recovery for all logs in data directory: " +
          dir.getAbsolutePath)
      } else {
        // log recovery itself is being performed by `Log` class during initialization
        brokerState.newState(RecoveringFromUncleanShutdown)
      }

      //读取recovery-point-offset-checkpoint文件
      val recoveryPoints = this.recoveryPointCheckpoints(dir).read

      val jobsForDir = for {
        dirContent <- Option(dir.listFiles).toList
        logDir <- dirContent if logDir.isDirectory
      } yield {
        Utils.runnable {
          debug("Loading log '" + logDir.getName + "'")

          val topicPartition = Log.parseTopicPartitionName(logDir.getName)
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)

          val current = new Log(logDir, config, logRecoveryPoint, scheduler, time)
          val previous = this.logs.put(topicPartition, current)

          if (previous != null) {
            throw new IllegalArgumentException(
              "Duplicate log directories found: %s, %s!".format(
              current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
          }
        }
      }

      jobs(cleanShutdownFile) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        cleanShutdownFile.delete()
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
    }

    info("Logs loading complete.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention", 
                         cleanupLogs, 
                         delay = InitialTaskDelayMs, 
                         period = retentionCheckMs, 
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher", 
                         flushDirtyLogs, 
                         delay = InitialTaskDelayMs, 
                         period = flushCheckMs, 
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointRecoveryPointOffsets,
                         delay = InitialTaskDelayMs,
                         period = flushCheckpointMs,
                         TimeUnit.MILLISECONDS)
    }
    if(cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      Utils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    for (dir <- this.logDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        Utils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        Utils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile())
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }


  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionAndOffsets Partition logs that need to be truncated
   */
  def truncateTo(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    for ((topicAndPartition, truncateOffset) <- partitionAndOffsets) {
      val log = logs.get(topicAndPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner: Boolean = (truncateOffset < log.activeSegment.baseOffset)
        if (needToStopCleaner && cleaner != null)
          cleaner.abortAndPauseCleaning(topicAndPartition)
        log.truncateTo(truncateOffset)
        if (needToStopCleaner && cleaner != null)
          cleaner.resumeCleaning(topicAndPartition)
      }
    }
    //每一个目录,对应一个recovery-point-offset-checkpoint文件,用于标示该目录下topic-partition同步到哪些offset了
    checkpointRecoveryPointOffsets()
  }

  /**
   *  Delete all data in a partition and start the log at the new offset
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(topicAndPartition: TopicAndPartition, newOffset: Long) {
    val log = logs.get(topicAndPartition)
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null)
        cleaner.abortAndPauseCleaning(topicAndPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null)
        cleaner.resumeCleaning(topicAndPartition)
    }
    
    //每一个目录,对应一个recovery-point-offset-checkpoint文件,用于标示该目录下topic-partition同步到哪些offset了
    checkpointRecoveryPointOffsets()
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory 
   * to avoid recovering the whole log on startup.
   * 每一个目录,对应一个recovery-point-offset-checkpoint文件,用于标示该目录下topic-partition同步到哪些offset了
   */
  def checkpointRecoveryPointOffsets() {
    this.logDirs.foreach(checkpointLogsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   * 向每一个目录更新一个recovery-point-offset-checkpoint文件,用于标示该目录下topic-partition同步到哪些offset了
   */
  private def checkpointLogsInDir(dir: File): Unit = {
    val recoveryPoints = this.logsByDir.get(dir.toString)
    if (recoveryPoints.isDefined) {
      //每一个目录,对应一个recovery-point-offset-checkpoint文件,用于标示该目录下topic-partition同步到哪些offset了
      this.recoveryPointCheckpoints(dir).write(recoveryPoints.get.mapValues(_.recoveryPoint))
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   * 获取topic-partition对应的Log对象
   */
  def getLog(topicAndPartition: TopicAndPartition): Option[Log] = {
    val log = logs.get(topicAndPartition)
    if (log == null)
      None
    else
      Some(log)
  }

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
   * 为一个topic-partiton创建一个LOG对象,如果存在则不需要创建
   */
  def createLog(topicAndPartition: TopicAndPartition, config: LogConfig): Log = {
    logCreationOrDeletionLock synchronized {
      var log = logs.get(topicAndPartition)
      
      // check if the log has already been created in another thread 如果存在,则不需要创建该LOG对象
      if(log != null)
        return log
      
      // if not, create it 为该topic-partiton对应的LOG对象寻找一个磁盘目录进行存储
      val dataDir = nextLogDir()
      //创建目录dataDir/topic-partition目录,用于存储该LOG的文件信息
      val dir = new File(dataDir, topicAndPartition.topic + "-" + topicAndPartition.partition)
      dir.mkdirs()
      log = new Log(dir, 
                    config,
                    recoveryPoint = 0L,
                    scheduler,
                    time)
      logs.put(topicAndPartition, log)
      info("Created log for partition [%s,%d] in %s with properties {%s}."
           .format(topicAndPartition.topic, 
                   topicAndPartition.partition, 
                   dataDir.getAbsolutePath,
                   {import JavaConversions._; config.toProps.mkString(", ")}))
      log
    }
  }

  /**
   *  Delete a log.
   *  删除topic-partition 对应的LOG文件对象
   */
  def deleteLog(topicAndPartition: TopicAndPartition) {
    var removedLog: Log = null//待删除的LOG对象
    
    //用锁进行同步,删除内部映射关系
    logCreationOrDeletionLock synchronized {
      removedLog = logs.remove(topicAndPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null) {
        cleaner.abortCleaning(topicAndPartition)
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      removedLog.delete()
      info("Deleted log for partition [%s,%d] in %s."
           .format(topicAndPartition.topic,
                   topicAndPartition.partition,
                   removedLog.dir.getAbsolutePath))
    }
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   * 从日志的dir目录集合中获取一个日志目录,原则是获取LOG对象数量最少的目录Path作为File对象返回
   */
  private def nextLogDir(): File = {
    if(logDirs.size == 1) {//如果仅有一个目录,则获取该目录返回即可
      logDirs(0)
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      //按照每一个log对象所在的目录分组,获取该目录中拥有多少个LOG对象,返回值key是file的所在目录path,value是该path下有多少个LOG对象
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap//初始化每一个默认为0个文件
      var dirCounts = (zeros ++ logCounts).toBuffer//合并两个集合,返回值key是file的所在目录path,value是该path下有多少个LOG对象
    
      // choose the directory with the least logs in it 按照LOG数量排序,获取拥有LOG最少的目录path
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Runs through the log removing segments older than a certain age
   * 保留segment文件最长时间,即segment文件最后修改时间超过了该阀值,则将其删除
   */
  private def cleanupExpiredSegments(log: Log): Int = {
    val startMs = time.milliseconds
    log.deleteOldSegments(startMs - _.lastModified > log.config.retentionMs)
  }

  /**
   *  Runs through the log removing segments until the size of the log
   *  is at least logRetentionSize bytes in size
   */
  private def cleanupSegmentsToMaintainSize(log: Log): Int = {
    if(log.config.retentionSize < 0 || log.size < log.config.retentionSize)
      return 0
    var diff = log.size - log.config.retentionSize//计算超出阀值多少字节
    
    //循环每一个LogSegment对象,将其删除
    def shouldDelete(segment: LogSegment) = {
      if(diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }
    log.deleteOldSegments(shouldDelete)
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * 定期删除segment文件,根据LogConfig的retentionMs和retentionSize属性进行删除segment文件
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log)
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   * 获取所有的log对象集合,每一个topic-partition对应一个该LOG对象
   */
  def allLogs(): Iterable[Log] = logs.values

  /**
   * Get a map of TopicAndPartition => Log
   * 获取 每一个topic-partition对应一个该LOG对象 映射关系
   */
  def logsByTopicPartition = logs.toMap

  /**
   * Map of log dir to logs by topic and partitions in that dir
   * 返回Map<String, Map[TopicAndPartition, Log]>
   *  key是文件的path,value是该path下面对应的topic-partition-log文件集合映射关系
   */
  private def logsByDir = {
    this.logsByTopicPartition.groupBy {
      case (_, log) => log.dir.getParent
    }
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   * 遍历所有的LOG对象,如果时间到了该flush的时间,则进行flush到磁盘
   */
  private def flushDirtyLogs() = {
    debug("Checking for dirty logs to flush...")

    for ((topicAndPartition, log) <- logs) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicAndPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicAndPartition.topic, e)
      }
    }
  }
}
