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

import kafka.message._
import kafka.common._
import kafka.utils._
import kafka.server.{LogOffsetMetadata, FetchDataInfo}

import scala.math._
import java.io.File


 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each 
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 * 
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file. 
 * 
 * @param log The message set containing log entries 存储message信息的文件流
 * @param index The offset index
 * @param baseOffset A lower bound on the offsets in this segment 代表该log的segment文件第一个message在整个队列中的序号offset
 * @param indexIntervalBytes The approximate number of bytes between entries in the index 代表索引间隔,每隔多少个字节建立一次索引
 * @param time The time instance 时间调度器
 */
@nonthreadsafe
class LogSegment(val log: FileMessageSet, //数据文件
                 val index: OffsetIndex,  //索引文件
                 val baseOffset: Long, //属于该topic-partition的第几个message,例如该值为100,则说明该文件LogSegment的第一个message编号是topic-partition的100号信息
                 val indexIntervalBytes: Int,//config.indexInterval 用于设置索引文件的间隔,多少间隔设置一个索引,该值是一个字节数
                 val rollJitterMs: Long,//config.randomSegmentJitter 用处不太大,用于校准时间戳的,在配置文件配置的,参见LogConfig.randomSegmentJitter
                 time: Time) extends Logging {
  
  var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index 记录上一次索引到现在已经多少个字节,当达到indexIntervalBytes时候,该值会重置为0*/
  private var bytesSinceLastIndexEntry = 0 //距离上一次索引文件写入后,有多少个字节新产生了
  
  /**
   * @param dir 代表该log的segment文件所在目录
   * @param startOffset 代表该log的segment文件第一个message在整个队列中的序号offset
   * @param indexIntervalBytes 代表索引间隔,每隔多少个字节建立一次索引
   * @param maxIndexSize 代表索引文件的最大字节数
   * @param
   * @param time 时间调度器
   * 
   */
  def this(dir: File, startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int, rollJitterMs: Long, time: Time) =
    this(new FileMessageSet(file = Log.logFilename(dir, startOffset)), 
         new OffsetIndex(file = Log.indexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
         startOffset,
         indexIntervalBytes,
         rollJitterMs,
         time)
    
  /* Return the size in bytes of this log segment 该segment日志中目前所占用字节总数*/
  def size: Long = log.sizeInBytes()
  
  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   * 
   * It is assumed this method is being called from within a lock.
   * 
   * @param offset The first offset in the message set. 在全部文档中该message属于第几个,注意不是在segment里,而是partition的全局中 
   * @param messages The messages to append.
   * 
   * 在该segment中插入一个message,并且适当情况下要更新索引文件
   */
  @nonthreadsafe
  def append(offset: Long, messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      //在第offset个message地方插入多少个字节作为message信息,在当前segment的第几个字节位置开始插入
      trace("Inserting %d bytes at offset %d at position %d".format(messages.sizeInBytes, offset, log.sizeInBytes()))
      // append an entry to the index (if needed)
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {//是否达到了写索引需求
        index.append(offset, log.sizeInBytes())//写入索引,参数1.在全部文档中该message属于第几个,注意不是在segment里,而是partition的全局中 2.在该segment的哪个节点位置开始插入该offset信息
        this.bytesSinceLastIndexEntry = 0//记录上一次索引到现在已经多少个字节,当达到indexIntervalBytes时候,该值会重置为0
      }
      // append the messages
      log.append(messages)//写入该message信息
      this.bytesSinceLastIndexEntry += messages.sizeInBytes
    }
  }
  
  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   * 
   * The lowerBound argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   * 
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * 
   * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): OffsetPosition = {
    val mapping = index.lookup(offset)
    log.searchFor(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   * 
   * @param startOffset A lower bound on the first offset to include in the message set we read 要读取的message的全局序号
   * @param maxOffset An optional maximum offset for the message set we read 如果我们设置了该值,表示最多读取到哪个全局的序号为止,就不读了
   * @param maxSize The maximum number of bytes to include in the message set we read最多读取多少个字节
   * 
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   * 从该segment文件中查获序号是startOffset的message信息
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int): FetchDataInfo = {
    if(maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    val logSize = log.sizeInBytes // this may change, need to save a consistent copy 这个值可能被更改.因此先进行备份
    val startPosition = translateOffset(startOffset)//查找该序号在文件中的位置偏移量

    // if the start position is already off the end of the log, return null
    if(startPosition == null)//说明没有找到该序号的message
      return null

    //创建该message序号对应的对象
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.position)

    // if the size is zero, still return a log segment but with zero size
    if(maxSize == 0)
      return FetchDataInfo(offsetMetadata, MessageSet.Empty)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    //返回值就是要读取多少字节
    val length = 
      maxOffset match {
        case None =>
          // no max offset, just use the max size they gave unmolested
          maxSize
        case Some(offset) => {//说明设置了最大序号
          // there is a max offset, translate it to a file position and use that to calculate the max read size
          if(offset < startOffset) //最大序号一定比开始序号要大
            throw new IllegalArgumentException("Attempt to read with a maximum offset (%d) less than the start offset (%d).".format(offset, startOffset))
          val mapping = translateOffset(offset, startPosition.position)//计算最大序号的字节所在位
          val endPosition = 
            if(mapping == null)//没有找到最大的序号,则读取到文件结尾
              logSize // the max offset is off the end of the log, use the end of the file
            else
              mapping.position//找到序号了,则只是截取这两个序号之间的message被返回
          min(endPosition - startPosition.position, maxSize) 
        }
      }

    //offsetMetadata表示开始的序号所在文件的字节位置、序号、文件的第一个序号信息
    //log.read(startPosition.position, length)表示读取log日志中从开始位置,读取多少个字节
    FetchDataInfo(offsetMetadata, log.read(startPosition.position, length))
  }
  
  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
   * 
   * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
   * is corrupt.
   * 
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(maxMessageSize: Int): Int = {
    index.truncate()
    index.resize(index.maxIndexSize)
    var validBytes = 0
    var lastIndexEntry = 0
    val iter = log.iterator(maxMessageSize)
    try {
      while(iter.hasNext) {
        val entry = iter.next
        entry.message.ensureValid()
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          // we need to decompress the message, if required, to get the offset of the first uncompressed message
          val startOffset =
            entry.message.compressionCodec match {
              case NoCompressionCodec =>
                entry.offset
              case _ =>
                ByteBufferMessageSet.decompress(entry.message).head.offset
          }
          index.append(startOffset, validBytes)
          lastIndexEntry = validBytes
        }
        validBytes += MessageSet.entrySize(entry.message)
      }
    } catch {
      case e: InvalidMessageException => 
        logger.warn("Found invalid messages in log segment %s at byte offset %d: %s.".format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    val truncated = log.sizeInBytes - validBytes
    log.truncateTo(validBytes)
    index.trimToValidSize()
    truncated
  }

  override def toString() = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    val mapping = translateOffset(offset)
    if(mapping == null)
      return 0
    index.truncateTo(offset)
    // after truncation, reset and allocate more space for the (new currently  active) index
    index.resize(index.maxIndexSize)
    val bytesTruncated = log.truncateTo(mapping.position)
    if(log.sizeInBytes == 0)
      created = time.milliseconds
    bytesSinceLastIndexEntry = 0
    bytesTruncated
  }
  
  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def nextOffset(): Long = {
    val ms = read(index.lastOffset, None, log.sizeInBytes)
    if(ms == null) {
      baseOffset
    } else {
      ms.messageSet.lastOption match {
        case None => baseOffset
        case Some(last) => last.nextOffset
      }
    }
  }
  
  /**
   * Flush this log segment to disk
   * flush日志文件和索引文件
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      index.flush()
    }
  }
  
  /**
   * Change the suffix for the index and log file for this log segment
   * 修改日志和索引文件的后缀名
   * 例如xxx.oldSuffix 最后返回xxx.newSuffix
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {
    val logRenamed = log.renameTo(new File(Utils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    if(!logRenamed)
      throw new KafkaStorageException("Failed to change the log file suffix from %s to %s for log segment %d".format(oldSuffix, newSuffix, baseOffset))
    val indexRenamed = index.renameTo(new File(Utils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
    if(!indexRenamed)
      throw new KafkaStorageException("Failed to change the index file suffix from %s to %s for log segment %d".format(oldSuffix, newSuffix, baseOffset))
  }
  
  /**
   * Close this log segment
   * 关闭日志和索引文件
   */
  def close() {
    Utils.swallow(index.close)
    Utils.swallow(log.close)
  }
  
  /**
   * Delete this log segment from the filesystem.
   * @throws KafkaStorageException if the delete fails.
   * 索引文件和log文件一起删除
   */
  def delete() {
    val deletedLog = log.delete() //删除日志文件.返回是否删除成功
    val deletedIndex = index.delete() //删除索引文件,返回是否删除成功
    if(!deletedLog && log.file.exists) //说明文件存在,但是删除不成功,则抛异常
      throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
    if(!deletedIndex && index.file.exists)
      throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
  }
  
  /**
   * The last modified time of this log segment as a unix time stamp
   * log文件的最后修改时间
   */
  def lastModified = log.file.lastModified
  
  /**
   * Change the last modified time for this log segment
   * 设置最后修改时间
   */
  def lastModified_=(ms: Long) = {
    log.file.setLastModified(ms)
    index.file.setLastModified(ms)
  }
}