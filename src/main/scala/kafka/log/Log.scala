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

import kafka.utils._
import kafka.message._
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{LogOffsetMetadata, FetchDataInfo, BrokerTopicStats}

import java.io.{IOException, File}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic._
import java.text.NumberFormat
import scala.collection.JavaConversions

import com.yammer.metrics.core.Gauge


/**
 * An append-only log for storing messages.
 * 
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 * 
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 * 
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock 
 * 
 * 每一个TopicAndPartition对应一个LOG对象
 * 
 */
@threadsafe
class Log(val dir: File,
          @volatile var config: LogConfig,//属于该topic-partition的配置文件,因为每个topic可能有不同的配置信息
          @volatile var recoveryPoint: Long = 0L,//每一个目录,对应一个recovery-point-offset-checkpoint文件,用于标示该目录下topic-partition同步到哪些offset了
          scheduler: Scheduler,
          time: Time = SystemTime) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* last time it was flushed 最后flush的时间戳*/
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  /** the actual segments of the log 
   * key是该log的segment的第一个序列号,value是LogSegment
   * 
    ConcurrentNavigableMap<Integer,String> map = new ConcurrentSkipListMap<Integer,String>();
    map.put(1, "111");
    map.put(3, "333");
    map.put(5, "555");
    map.put(7, "777");
    
    System.out.println(map.subMap(1, true, 7, false));//{1=111, 3=333, 5=555}
    System.out.println(map.subMap(1, true, 7, true));//{1=111, 3=333, 5=555, 7=777}
    
    System.out.println(map.subMap(2, true, 7, false));//{3=333, 5=555}
    System.out.println(map.subMap(2, true, 7, true));//{3=333, 5=555, 7=777}
    * 
    System.out.println(map.headMap(2));//{1=111}
    System.out.println(map.headMap(3));//{1=111}
    System.out.println(map.headMap(1));//{}
    *
    * 找到距离key最小的数据,如果遇到相等的,就直接返回相等的数据
    System.out.println(map.floorKey(0));//null
    System.out.println(map.floorKey(2));//1
    System.out.println(map.floorKey(3));//3
    * 
    * System.out.println(map.lastEntry().getValue());//777
   **/
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]
  loadSegments()
  
  /* Calculate the offset of the next message */
  @volatile var nextOffsetMetadata = new LogOffsetMetadata(activeSegment.nextOffset(), activeSegment.baseOffset, activeSegment.size.toInt)

  val topicAndPartition: TopicAndPartition = Log.parseTopicPartitionName(name)//name为topic-partition组成TopicAndPartition对象的字符串

  info("Completed load of log %s with log end offset %d".format(name, logEndOffset))

  val tags = Map("topic" -> topicAndPartition.topic, "partition" -> topicAndPartition.partition.toString)

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  /** The name of this log */
  def name  = dir.getName()

  /* Load the log segments from the log files on disk
   * 从磁盘上加载log的segments
   **/
  private def loadSegments() {
    // create the log directory if it doesn't exist
    dir.mkdirs()
    
    // first do a pass through the files in the log directory and remove any temporary files 
    // and complete any interrupted swap operations
    //初始化删除一些临时文件和交换文件操作
    for(file <- dir.listFiles if file.isFile) {
      if(!file.canRead)
        throw new IOException("Could not read file " + file)
      val filename = file.getName
      if(filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {//.deleted 或者 .cleaned结尾的文件,进行删除
        // if the file ends in .deleted or .cleaned, delete it
        file.delete()
      } else if(filename.endsWith(SwapFileSuffix)) {//.swap结尾的文件
        // we crashed in the middle of a swap operation, to recover:
        // if a log, swap it in and delete the .index file
        // if an index just delete it, it will be rebuilt
        val baseName = new File(Utils.replaceSuffix(file.getPath, SwapFileSuffix, "")) //xxx.swap 转换成 xxx
        if(baseName.getPath.endsWith(IndexFileSuffix)) {//.index结尾的文件,删除掉
          file.delete()
        } else if(baseName.getPath.endsWith(LogFileSuffix)){//.log结尾的文件
          // delete the index
          val index = new File(Utils.replaceSuffix(baseName.getPath, LogFileSuffix, IndexFileSuffix)) //找到xxx.index,然后删除该文件
          index.delete()
          // complete the swap operation
          val renamed = file.renameTo(baseName) //将file转换成xxx.swap
          if(renamed)
            info("Found log file %s from interrupted swap operation, repairing.".format(file.getPath))
          else
            throw new KafkaException("Failed to rename file %s.".format(file.getPath))
        }
      }
    }

    // now do a second pass and load all the .log and .index files 再次循环一次,加载所有的log和index文件
    for(file <- dir.listFiles if file.isFile) {
      val filename = file.getName
      if(filename.endsWith(IndexFileSuffix)) {//.index结尾的文件
        // if it is an index file, make sure it has a corresponding .log file 确保该索引文件对应的log文件也存在
        val logFile = new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))//找到xxx.log文件
        if(!logFile.exists) {//如果对应的log文件不存在,则删除该索引文件
          warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
          file.delete()
        }
      } else if(filename.endsWith(LogFileSuffix)) {
        // if its a log file, load the corresponding log segment,获取该log文件的segment的num
        val start = filename.substring(0, filename.length - LogFileSuffix.length).toLong
        val hasIndex = Log.indexFilename(dir, start).exists//判断该索引文件是否存在
        val segment = new LogSegment(dir = dir, 
                                     startOffset = start,
                                     indexIntervalBytes = config.indexInterval, 
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time)
        if(!hasIndex) {//如果索引文件不存在
          error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
          segment.recover(config.maxMessageSize)
        }
        segments.put(start, segment)
      }
    }

    if(logSegments.size == 0) {
      // no existing segments, create a new mutable segment beginning at offset 0
      segments.put(0L, new LogSegment(dir = dir,
                                     startOffset = 0,
                                     indexIntervalBytes = config.indexInterval, 
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time))
    } else {
      recoverLog()
      // reset the index size of the currently active log segment to allow more entries
      activeSegment.index.resize(config.maxIndexSize)
    }

    // sanity check the index file of every segment to ensure we don't proceed with a corrupt segment
    for (s <- logSegments)
      s.index.sanityCheck()
  }

  //更新全局下一个最大的message的序号
  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size.toInt)
  }
  
  private def recoverLog() {
    // if we have the clean shutdown marker, skip recovery 说明不需要恢复操作
    if(hasCleanShutdownFile) {
      this.recoveryPoint = activeSegment.nextOffset
      return
    }

    // okay we need to actually recovery this log
    val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
    while(unflushed.hasNext) {
      val curr = unflushed.next
      info("Recovering unflushed segment %d in log %s.".format(curr.baseOffset, name))
      val truncatedBytes = 
        try {
          curr.recover(config.maxMessageSize)
        } catch {
          case e: InvalidOffsetException => 
            val startOffset = curr.baseOffset
            warn("Found invalid offset during recovery for log " + dir.getName +". Deleting the corrupt segment and " +
                 "creating an empty one with starting offset " + startOffset)
            curr.truncateTo(startOffset)
        }
      if(truncatedBytes > 0) {
        // we had an invalid message, delete all remaining log
        warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(curr.baseOffset, name, curr.nextOffset))
        unflushed.foreach(deleteSegment)
      }
    }
  }
  
  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile() = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * 现在存在多少个日志文件
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size
  
  /**
   * Close this log
   */
  def close() {
    debug("Closing log " + name)
    lock synchronized {
      for(seg <- logSegments)
        seg.close()
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   * 
   * This method will generally be responsible for assigning offsets to the messages, 
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   * 
   * @param messages The message set to append
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   *                       true表示该参数自动给message分配序号,false表示盲目的使用message给定的序号,只要校验序号合法即可使用
   * 
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * 
   * @return Information about the appended messages including the first and last offset.
   */
  def append(messages: ByteBufferMessageSet, assignOffsets: Boolean = true): LogAppendInfo = {
    val appendInfo = analyzeAndValidateMessageSet(messages) //分析和校验 message集合
    
    // if we have any valid messages, append them to the log 如果我们没有任何有效的message,则直接返回该统计信息即可,不需要添加到日志中
    if(appendInfo.shallowCount == 0)
      return appendInfo
      
    // trim any invalid bytes or partial messages before appending it to the on-disk log
      //返回一个仅仅包含有效字节数的ByteBufferMessageSet对象
    var validMessages = trimInvalidBytes(messages, appendInfo)

    try {
      // they are valid, insert them in the log
      lock synchronized {
        appendInfo.firstOffset = nextOffsetMetadata.messageOffset//设置第一个序号为当前序号

        if(assignOffsets) {//自动分配序号
          // assign offsets to the message set如果不通过message决定,则重新为message分配序号
          val offset = new AtomicLong(nextOffsetMetadata.messageOffset)//从当前序号开始,生成计数器
          try {
            validMessages = validMessages.assignOffsets(offset, appendInfo.codec) //重新生成序号
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          appendInfo.lastOffset = offset.get - 1//因为序号被重新设置了,因此重新要设置最后一个message的序号是多少
        } else {
          // we are taking the offsets we are given 如果按照参数message的序号的话,需要校验条件,如果给定的序号不是单调增长的,是不允许的;如果第一个message的序号都比我们现在存在的序号都小,也是不可以的,必须比我们存在的序号要大,因此只能出现空的序号,不会出现问题
          if(!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + messages)
        }

        // re-validate message sizes since after re-compression some may exceed the limit
        //浅遍历每一个message信息,校验每一个message的信息大小是否超过了限制
        for(messageAndOffset <- validMessages.shallowIterator) {
          if(MessageSet.entrySize(messageAndOffset.message) > config.maxMessageSize) {
            // we record the original message set size instead of trimmed size
            // to be consistent with pre-compression bytesRejectedRate recording
            BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
            BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
            throw new MessageSizeTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
              .format(MessageSet.entrySize(messageAndOffset.message), config.maxMessageSize))
          }
        }

        // check messages set size may be exceed config.segmentSize
        if(validMessages.sizeInBytes > config.segmentSize) {//校验message的信息大小,可能超过了整个segment文件的大小,也是不允许的
          throw new MessageSetSizeTooLargeException("Message set size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validMessages.sizeInBytes, config.segmentSize))
        }


        // maybe roll the log if this segment is full 找到要写入的日志文件
        val segment = maybeRoll(validMessages.sizeInBytes)

        // now append to the log 向日志文件中追加该message集合
        segment.append(appendInfo.firstOffset, validMessages)

        // increment the log end offset 更新下一个message的序号
        updateLogEndOffset(appendInfo.lastOffset + 1)

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
                .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validMessages))

        if(unflushedMessages >= config.flushInterval) //达到了flush的条数,则进行flush操作
          flush()

        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }
  
  /**
   * Struct to hold various quantities we compute about each message set before appending to the log
   * 在追加到log日志前,我们要进行计算每一个message,构建一个各种各样的数量
   * @param firstOffset The first offset in the message set 在message集合中第一个偏移量
   * @param lastOffset The last offset in the message set 在message集合中最后一个偏移量
   * @param shallowCount The number of shallow messages 一共多少个浅遍历的message信息
   * @param validBytes The number of valid bytes 一共占用多少个有效的字节
   * @param codec The codec used in the message set message需要什么编码
   * @param offsetsMonotonic Are the offsets in this message set monotonically increasing,offset属性是否是单调递增的
   */
  case class LogAppendInfo(var firstOffset: Long, var lastOffset: Long, codec: CompressionCodec, shallowCount: Int, validBytes: Int, offsetsMonotonic: Boolean)

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * </ol>
   * 
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateMessageSet(messages: ByteBufferMessageSet): LogAppendInfo = {
    var shallowMessageCount = 0 //浅遍历message的数量
    var validBytesCount = 0 //有效字节数
    var firstOffset, lastOffset = -1L
    var codec: CompressionCodec = NoCompressionCodec
    var monotonic = true //ID是否是单调递增
    for(messageAndOffset <- messages.shallowIterator) {
      // update the first offset if on the first message
      if(firstOffset < 0)
        firstOffset = messageAndOffset.offset
      // check that offsets are monotonically increasing
      if(lastOffset >= messageAndOffset.offset)
        monotonic = false
      // update the last offset seen
      lastOffset = messageAndOffset.offset

      val m = messageAndOffset.message

      // Check if the message sizes are valid.
      val messageSize = MessageSet.entrySize(m)
      if(messageSize > config.maxMessageSize) {//message的大小超过了最大限制,则抛异常
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
        throw new MessageSizeTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
          .format(messageSize, config.maxMessageSize))
      }

      // check the validity of the message by checking CRC
      m.ensureValid()//校验和是否正确

      shallowMessageCount += 1
      validBytesCount += messageSize
      
      val messageCodec = m.compressionCodec
      if(messageCodec != NoCompressionCodec)
        codec = messageCodec
    }
    LogAppendInfo(firstOffset, lastOffset, codec, shallowMessageCount, validBytesCount, monotonic)
  }
  
  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   * 修改任何有效的字节
   * @param messages The message set to trim 所有的message集合
   * @param info The general information of the message set 有效的message字节数
   * @return A trimmed message set. This may be the same as what was passed in or it may not.返回一个可以仅仅存储有效字节的ByteBufferMessageSet,有可能就是参数的ByteBufferMessageSet一样
   */
  private def trimInvalidBytes(messages: ByteBufferMessageSet, info: LogAppendInfo): ByteBufferMessageSet = {
    val messageSetValidBytes = info.validBytes //有效的字节数
    if(messageSetValidBytes < 0)
      throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if(messageSetValidBytes == messages.sizeInBytes) {//说明message集合全部字节都有效
      messages
    } else {//说明message集合中部分字节有效
      // trim invalid bytes
      val validByteBuffer = messages.buffer.duplicate()//复制整个message集合
      validByteBuffer.limit(messageSetValidBytes)//设置有效长度
      new ByteBufferMessageSet(validByteBuffer)
    }
  }

  /**
   * Read messages from the log
   * 从日志文件中读取message信息
   * @param startOffset The offset to begin reading at 从什么全局的序号开始读取日志信息
   * @param maxLength The maximum number of bytes to read 最多读取多少个字节数
   * @param maxOffset -The offset to read up to, exclusive. (i.e. the first offset NOT included in the resulting message set).最多读取到哪个序号message结束
   * 
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
   * @return The fetch data information including fetch starting offset metadata and messages read
   * 从startOffset位置开始读取,最多读取maxLength个字节
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None): FetchDataInfo = {
    //打印日志,最多读取多少个字节,从开始字节xx开始读取,读取什么partition日志文件,当前日志文件大小是多少
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // check if the offset is valid and in range 获取下一个message的序号
    val next = nextOffsetMetadata.messageOffset
    if(startOffset == next)//说明没有数据可以被抓去,因为要抓去的序号是未来的下一个序号
      return FetchDataInfo(nextOffsetMetadata, MessageSet.Empty)
    
    var entry = segments.floorEntry(startOffset) //获取小于等于startOffset的LogSegment文件对象
      
    // attempt to read beyond the log end offset is an error超出范围了
    if(startOffset > next || entry == null)//打印日志,要请求的序号是xxx,但是我们目前只有序号在xxx到xxx范围内的序号message信息
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))
    
    // do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(entry != null) {
      val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength)//在该segment中查找指定序号的message信息
      if(fetchInfo == null) {//说明没有找到我们要的序号message信息
        entry = segments.higherEntry(entry.getKey)//则去下一个segment中继续查找,直到查找到最后没有segment为止
      } else {
        return fetchInfo//说明找到了,则返回
      }
    }
    
    // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
    // this can happen when all messages with offset larger than start offsets have been deleted.
    // In this case, we will return the empty set with log end offset metadata
    FetchDataInfo(nextOffsetMetadata, MessageSet.Empty)
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   * 找到序号message的一些元数据
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = read(offset, 1)
      fetchDataInfo.fetchOffset
    } catch {
      case e: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,删除任何匹配predicate函数的日志文件
   * starting with the oldest segment and moving forward until a segment doesn't match.
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable 判定predicate函数参数是LogSegment,返回值是一个boolean类型的
   * @return The number of segments deleted
   * 删除符合该LogSegment作为参数的方法的所有文件
   * 
   */
  def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    // find any segments that match the user-supplied predicate UNLESS it is the final segment 
    // and it is empty (since we would just end up re-creating it
    val lastSegment = activeSegment//最后一个文件
    //循环所有的文件,只要predicate函数返回的是true,则作为删除的目标
    val deletable = logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastSegment.baseOffset || s.size > 0))
    val numToDelete = deletable.size //等待删除多少个文件
    if(numToDelete > 0) {
      lock synchronized {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if(segments.size == numToDelete) //如果要全部都删除了,则要创建一个新的
          roll()
        // remove the segments for lookups
        deletable.foreach(deleteSegment(_))//真正去删除每一个文件
      }
    }
    numToDelete//返回删除了多少个文件
  }

  /**
   * The size of the log in bytes
   * 该LOG文件所有的segment文件的字节大小之和
   */
  def size: Long = logSegments.map(_.size).sum

   /**
   * The earliest message offset in the log
   * 当前第一个序号
   */
  def logStartOffset: Long = logSegments.head.baseOffset

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   *  The offset of the next message that will be appended to the log
   *  下一个message信息要占用的编号
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes
   * logSegment will be rolled if one of the following conditions met
   *
   * 判断满的条件
a.文件存储到额字节数已经够了
b.文件创建的时间超时了
c.索引文件满了
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   * 尝试是否更换一个文件,否则返回最后一个文件即可
   */
  private def maybeRoll(messagesSize: Int): LogSegment = {
    val segment = activeSegment//获取当前最后一个日志文件
    
    if (segment.size > config.segmentSize - messagesSize || //说明logSegment文件满了,该更换了
        segment.size > 0 && time.milliseconds - segment.created > config.segmentMs - segment.rollJitterMs || //一个LogSegment文件创建超过该时间,则不管日志是否满了,都创建一个新的日志文件
        segment.index.isFull) {//索引文件满了
      
      //滚动创建一个新的日志文件
      debug("Rolling new log segment in %s (log_size = %d/%d, index_size = %d/%d, age_ms = %d/%d)."
            .format(name,
                    segment.size,
                    config.segmentSize,
                    segment.index.entries,
                    segment.index.maxEntries,
                    time.milliseconds - segment.created,
                    config.segmentMs - segment.rollJitterMs))
      roll()
    } else {//返回当前最后一个日志文件
      segment
    }
  }
  
  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   * @return The newly rolled segment
   * 滚动生成一个新的文件,返回新生成的文件
   */
  def roll(): LogSegment = {
    val start = time.nanoseconds
    lock synchronized {
      val newOffset = logEndOffset//下一个message的编号
      val logFile = logFilename(dir, newOffset)//创建log文件,例如dir/00000000000000000001.log
      val indexFile = indexFilename(dir, newOffset)//创建index文件,例如dir/00000000000000000001.index
      for(file <- List(logFile, indexFile); if file.exists) {//如果这两个文件存在,则删除掉,理论上不应该存在
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        file.delete()
      }
    
      //更新最后一个segment的index.trimToValidSize(),让索引文件没有多余的字节空位
      segments.lastEntry() match {
        case null => 
        case entry => entry.getValue.index.trimToValidSize()
      }
      
      //创建新的segment文件
      val segment = new LogSegment(dir, 
                                   startOffset = newOffset,//设置当前topic-partition已经记录第几个message了
                                   indexIntervalBytes = config.indexInterval, //设置索引间隔
                                   maxIndexSize = config.maxIndexSize,
                                   rollJitterMs = config.randomSegmentJitter,
                                   time = time)
      val prev = addSegment(segment)//将新增加的LogSegment对象添加到内部映射中
      if(prev != null) //以前绝对不能存在,因此如果存在会抛异常
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
      
      // schedule an asynchronous flush of the old segment
      scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L) //将老的信息刷新到磁盘
      
      //滚动产生一个新的日志文件
      info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0*1000.0)))
      
      //返回新的日志文件
      segment
    }
  }
  
  /**
   * The number of messages appended to the log since the last flush
   * 计算多少个message信息没有追加到日志系统中
   * 即下一个message的位置-上一次flush的位置,剩下的就是没有被flush到磁盘的message数量
   */
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint
  
  /**
   * Flush all log segments
   * 刷新全部的信息
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   * 刷新所有日志文件,一直到offset-1的位置
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long) : Unit = {
    if (offset <= this.recoveryPoint) //如果offset = 50,recoveryPoint = 100.说明目前日志已经刷新到磁盘的记录都已经到100了,因此不需要刷新
      return
    debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushedMessages)
    for(segment <- logSegments(this.recoveryPoint, offset)) //找到从上一次刷新到磁盘最后一个位置,到offset之间所有的日志文件,都已经刷新
      segment.flush()
    lock synchronized {
      if(offset > this.recoveryPoint) {//记录最新的刷新到的位置和刷新时间
        this.recoveryPoint = offset
        lastflushedTime.set(time.milliseconds)
      }
    }
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   * 删除所有的属于该partition的所有日志
   */
  private[log] def delete() {
    lock synchronized {
      logSegments.foreach(_.delete())//同步删除所有的segment
      segments.clear()
      Utils.rm(dir)//删除该topic-partiton对应的目录
    }
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   * 截短这个日志,到指定的位置即可
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete. 参数序号之后的数据都不要了
   */
  private[log] def truncateTo(targetOffset: Long) {
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset > logEndOffset) {  //假设目前的序号是100,参数是设置105,也就是要截取到105的位置,而现在还没有达到105的位置呢,因此是不需要截断操作的
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    lock synchronized {
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {//假设现在最小的序号是100,而参数是90,说明90以后都不要了,因此全部都不要了
        truncateFullyAndStartAt(targetOffset)//说明全部都不要了
      } else {
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset) //找到比要截取的临界值还大的,则删除掉这些日志
        deletable.foreach(deleteSegment(_))//循环删除每一个日志文件
        activeSegment.truncateTo(targetOffset)//设置删除后的最后一个日志,位置设置到截短的位置,之后的日志不要了
        updateLogEndOffset(targetOffset)//更新下一个序号的位置是targetOffset
        this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
      }
    }
  }
    
  /**
   *  Delete all data in the log and start at the new offset
   *  @param newOffset The new offset to start the log with
   *  删除全部日志,并且重新创建一个LogSegment,设置开始位置是newOffset
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    debug("Truncate and start log '" + name + "' to " + newOffset)
    lock synchronized {
      val segmentsToDelete = logSegments.toList//循环删除所有的日志
      segmentsToDelete.foreach(deleteSegment(_))
      addSegment(new LogSegment(dir, 
                                newOffset,
                                indexIntervalBytes = config.indexInterval, 
                                maxIndexSize = config.maxIndexSize,
                                rollJitterMs = config.randomSegmentJitter,
                                time = time))//增加一个新的segment
      updateLogEndOffset(newOffset)//更新下一个序号的位置是targetOffset
      this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   * 最后一次flush该LOG对象到磁盘的时间
   */
  def lastFlushTime(): Long = lastflushedTime.get
  
  /**
   * The active segment that is currently taking appends
   * 获取按照key排序后,segments最后一个元素的value值
   */
  def activeSegment = segments.lastEntry.getValue
  
  /**
   * All the log segments in this log ordered from oldest to newest
   * 属于该partition的所有segment集合
   */
  def logSegments: Iterable[LogSegment] = {
    import JavaConversions._
    segments.values
  }
  
  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
   * 获取从from--to之间的映射Map
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    import JavaConversions._
    lock synchronized {
      val floor = segments.floorKey(from)//获取比from小的key,最接近的
      if(floor eq null) //说明没有比from还小的key了
        segments.headMap(to).values //返回所有比to还小的key映射Map
      else
        segments.subMap(floor, true, to, false).values //获取从from--to之间的映射Map
    }
  }
  
  override def toString() = "Log(" + dir + ")"
  
  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   * 
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
    lock synchronized {
      segments.remove(segment.baseOffset)//删除映射关系
      asyncDeleteSegment(segment)//异步删除文件
    }
  }
  
  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   * @throws KafkaStorageException if the file can't be renamed and still exists 
   * 使用线程异步删除文件
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    def deleteSeg() {
      info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
      segment.delete()
    }
    scheduler.schedule("delete-file", deleteSeg, delay = config.fileDeleteDelayMs)
  }
  
  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
   * 
   * @param newSegment The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * 用于clean方式清理日志
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment]) {
    lock synchronized {
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      //将xxx.cleaned 改成xxx.swap
      newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      addSegment(newSegment)//添加新的segment
        
      // delete the old files 删除老的segment
      for(seg <- oldSegments) {
        // remove the index entry
        if(seg.baseOffset != newSegment.baseOffset)
          segments.remove(seg.baseOffset)//删除映射关系
        // delete segment
        asyncDeleteSegment(seg)//异步删除
      }
      // okay we are safe now, remove the swap suffix
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")//取消swap后缀
    }  
  }
  
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)
  
}

/**
 * Helper functions for logs
 */
object Log {
  
  /** a log file 日志文件后缀,命名规则segmengNum.log,eg:22.log*/
  val LogFileSuffix = ".log"
    
  /** an index file 索引文件后缀*/
  val IndexFileSuffix = ".index"
    
  /** a file that is scheduled to be deleted 说明该文件已经被调度,一会就在多线程中被删除了 */
  val DeletedFileSuffix = ".deleted"
    
  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"
    
  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
    * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
  /** TODO: Get rid of CleanShutdownFile in 0.8.2
    * 该日志指示了该broker服务器是clean shutdown的,即主动进行shutdow的,不需要还原恢复操作
    * */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   * @param offset The offset to use in the file name
   * @return The filename
   * 讲offset转换成20位的数字,开头以0补位
   * 例如offset = 1 则结果为00000000000000000001
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }
  
  /**
   * Construct a log file name in the given dir with the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * 在dir目录下创建一个日志文件,文件以偏移量命名,后缀为.log
   * 例如偏移量为1,则结果为dir/00000000000000000001.log
   */
  def logFilename(dir: File, offset: Long) = 
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)
  
  /**
   * Construct an index file name in the given dir using the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * 在dir目录下创建一个索引文件,文件以偏移量命名,后缀为.index
   * 例如偏移量为1,则结果为dir/00000000000000000001.index
   */
  def indexFilename(dir: File, offset: Long) = 
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)
  

  /**
   * Parse the topic and partition out of the directory name of a log
   * topic-partition组成TopicAndPartition对象的字符串
   * 参数格式topic-partition,转换成TopicAndPartition对象
   */
  def parseTopicPartitionName(name: String): TopicAndPartition = {
    val index = name.lastIndexOf('-')
    TopicAndPartition(name.substring(0,index), name.substring(index+1).toInt)
  }
}
  
