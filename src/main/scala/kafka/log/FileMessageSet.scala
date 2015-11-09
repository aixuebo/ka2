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
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._

import kafka.utils._
import kafka.message._
import kafka.common.KafkaException
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}

/**
 * An on-disk message set. An optional start and end position can be applied to the message set
 * which will allow slicing a subset of the file.
 * @param file The file name for the underlying log data
 * @param channel the underlying file channel used
 * @param start A lower bound on the absolute position in the file from which the message set begins
 * @param end The upper bound on the absolute position in the file at which the message set ends
 * @param isSlice Should the start and end parameters be used for slicing?
 * 
 * 该类表示在文件中存储若干个message对象,即存储message对象集合的文件
 * 每一个topic-partion对应一个该文件
 */
@nonthreadsafe
class FileMessageSet private[kafka](@volatile var file: File,
                                    private[log] val channel: FileChannel,
                                    private[log] val start: Int,
                                    private[log] val end: Int,
                                    isSlice: Boolean) extends MessageSet with Logging {
  
  /* the size of the message set in bytes ,当前message总字节大小*/
  private val _size = 
    if(isSlice)
      new AtomicInteger(end - start) // don't check the file size if this is just a slice view
    else
      new AtomicInteger(math.min(channel.size().toInt, end) - start)

  /* if this is not a slice, update the file pointer to the end of the file */
  if (!isSlice) 
    /* set the file position to the last byte in the file */
    channel.position(channel.size)//设置position,使后来加入的字节都是追加操作

  /**
   * Create a file message set with no slicing.
   */
  def this(file: File, channel: FileChannel) = 
    this(file, channel, start = 0, end = Int.MaxValue, isSlice = false)
  
  /**
   * Create a file message set with no slicing
   */
  def this(file: File) = 
    this(file, Utils.openChannel(file, mutable = true))

  /**
   * Create a file message set with mutable option
   */
  def this(file: File, mutable: Boolean) = this(file, Utils.openChannel(file, mutable))
  
  /**
   * Create a slice view of the file message set that begins and ends at the given byte offsets
   */
  def this(file: File, channel: FileChannel, start: Int, end: Int) = 
    this(file, channel, start, end, isSlice = true)
  
  /**
   * Return a message set which is a view into this set starting from the given position and with the given size limit.
   * 
   * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
   * 
   * If this message set is already sliced, the position will be taken relative to that slicing.
   * 
   * @param position The start position to begin the read from
   * @param size The number of bytes after the start position to include
   * 
   * @return A sliced wrapper on this message set limited based on the given position and size
   * 返回一个子集
   */
  def read(position: Int, size: Int): FileMessageSet = {
    if(position < 0)
      throw new IllegalArgumentException("Invalid position: " + position)
    if(size < 0)
      throw new IllegalArgumentException("Invalid size: " + size)
    new FileMessageSet(file,
                       channel,
                       start = this.start + position,
                       end = math.min(this.start + position + size, sizeInBytes()))
  }
  
  /**
   * Search forward for the file position of the last offset that is greater than or equal to the target offset
   * and return its physical position. If no such offsets are found, return null.
   * @param targetOffset The offset to search for.
   * @param startingPosition The starting position in the file to begin searching from.
   * 从startingPosition位置开始搜索查询,备注该startingPosition位置一定是一个message的头,即可以读取连续12个字节表示 该message在log中的偏移量和message在log中所占用字节
   * targetOffset 表示找到该log中第targetOffset个message为止
   * 返回OffsetPosition,该对象表示log日志中一个message信息
   */
  def searchFor(targetOffset: Long, startingPosition: Int): OffsetPosition = {
    var position = startingPosition
    val buffer = ByteBuffer.allocate(MessageSet.LogOverhead)//分配12个字节
    val size = sizeInBytes()//当前文件总字节大小
    while(position + MessageSet.LogOverhead < size) {//只要还存在message就不断循环
      buffer.rewind()
      channel.read(buffer, position)//读取该message在log中的偏移量和message在log中所占用字节
      if(buffer.hasRemaining)//如果没读取完.说明有异常
        throw new IllegalStateException("Failed to read complete buffer for targetOffset %d startPosition %d in %s"
                                        .format(targetOffset, startingPosition, file.getAbsolutePath))
      buffer.rewind()
      val offset = buffer.getLong()//读取该message在log中的偏移量
      if(offset >= targetOffset)//找到目标偏移量,即在log中第几个message信息
        return OffsetPosition(offset, position)
      val messageSize = buffer.getInt()//message在log中所占用字节
      if(messageSize < Message.MessageOverhead)//message字节存储数量有问题
        throw new IllegalStateException("Invalid message size: " + messageSize)
      position += MessageSet.LogOverhead + messageSize//进行一个message查询
    }
    null
  }
  
  /**
   * Write some of this set to the given channel.
   * @param destChannel The channel to write to.
   * @param writePosition The position in the message set to begin writing from.从哪个位置开始写
   * @param size The maximum number of bytes to write 最多写入多少个字节
   * @return The number of bytes actually written.真实的写入多少个字节
   * 将message信息集合中的信息写入到指定的channel中
   */
  def writeTo(destChannel: GatheringByteChannel, writePosition: Long, size: Int): Int = {
    // Ensure that the underlying size has not changed.
    val newSize = math.min(channel.size().toInt, end) - start//该文件目前真实的字节大小
    if (newSize < _size.get()) {
      throw new KafkaException("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d"
        .format(file.getAbsolutePath, _size.get(), newSize))
    }
    //将message信息集合中的信息写入到指定的channel中
    val bytesTransferred = channel.transferTo(start + writePosition, math.min(size, sizeInBytes), destChannel).toInt
    trace("FileMessageSet " + file.getAbsolutePath + " : bytes transferred : " + bytesTransferred
      + " bytes requested for transfer : " + math.min(size, sizeInBytes))
    bytesTransferred
  }
  
  /**
   * Get a shallow iterator over the messages in the set.
   * 从头读取整个文件,返回每一个message对象
   */
  override def iterator() = iterator(Int.MaxValue)
    
  /**
   * Get an iterator over the messages in the set. We only do shallow iteration here.
   * @param maxMessageSize A limit on allowable message size to avoid allocating unbounded memory. 
   * If we encounter a message larger than this we throw an InvalidMessageException.
   * @return The iterator.
   * 参数maxMessageSize 表示每一个message最大允许多少个字节
   * 
   * 从头读取整个文件,返回每一个message对象
   */
  def iterator(maxMessageSize: Int): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var location = start//初始化开始位置
      val sizeOffsetBuffer = ByteBuffer.allocate(12)
      
      override def makeNext(): MessageAndOffset = {
        if(location >= end)
          return allDone()
          
        // read the size of the item
        sizeOffsetBuffer.rewind()
        channel.read(sizeOffsetBuffer, location)
        if(sizeOffsetBuffer.hasRemaining)//理论上一次性可以读取12个字节,代表该message的偏移量8个字节和该message的字节总数4个字节,但是读取完后依然有数据,说明有问题,表示全部读取完成,因此调用allDone方法
          return allDone()
        
        sizeOffsetBuffer.rewind()
        val offset = sizeOffsetBuffer.getLong()//代表该message的偏移量8个字节
        val size = sizeOffsetBuffer.getInt()//该message的字节总数4个字节
        if(size < Message.MinHeaderSize)
          return allDone()//说明读取完了,已经没有数据了
        if(size > maxMessageSize)//超出限制,抛异常
          throw new InvalidMessageException("Message size exceeds the largest allowable message size (%d).".format(maxMessageSize))
        
        // read the item itself
        val buffer = ByteBuffer.allocate(size)//读取message全部信息
        channel.read(buffer, location + 12)//从12个字节之后读取size大小的buffer,表示读取message的正文
        if(buffer.hasRemaining)//理论上message必须读取完,没有剩余空间,如果存在剩余空间,则说明读取全部完成
          return allDone()
        buffer.rewind()//重置buffer的position为0,方便后续操作本次读取的buffer正文
        
        // increment the location and return the item
        location += size + 12//location设置到下一个message位置
        new MessageAndOffset(new Message(buffer), offset)
      }
    }
  }
  
  /**
   * The number of bytes taken up by this file set
   */
  def sizeInBytes(): Int = _size.get()
  
  /**
   * Append these messages to the message set
   * 将messages全部内容写入到channel中
   */
  def append(messages: ByteBufferMessageSet) {
    val written = messages.writeTo(channel, 0, messages.sizeInBytes)
    _size.getAndAdd(written)
  }
 
  /**
   * Commit all written data to the physical disk
   */
  def flush() = {
    channel.force(true)
  }
  
  /**
   * Close this message set
   */
  def close() {
    flush()
    channel.close()
  }
  
  /**
   * Delete this message set from the filesystem
   * @return True iff this message set was deleted.
   */
  def delete(): Boolean = {
    Utils.swallow(channel.close())
    file.delete()
  }

  /**
   * Truncate this file message set to the given size in bytes. Note that this API does no checking that the 
   * given size falls on a valid message boundary.
   * @param targetSize The size to truncate to.
   * @return The number of bytes truncated off
   * 截断文件,这个方法不保证截断位置的Message的完整性
   * 
   * 截取文件从0到targetSize字节之间,targetSize之后的文件删除,返回本次操作截取了多少个字节
   */
  def truncateTo(targetSize: Int): Int = {
    val originalSize = sizeInBytes//原始文件大小
    if(targetSize > originalSize || targetSize < 0)//要保证0<targe<originalSize
      throw new KafkaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                               " size of this log segment is " + originalSize + " bytes.")
    channel.truncate(targetSize)//截取文件时，文件将中指定长度后面的部分将被删除
    channel.position(targetSize)//定位位置为截取后的位置,使输出流可以继续追加
    _size.set(targetSize)//更新原始文件大小,现在原始文件就是参数目标文件大小
    originalSize - targetSize//返回值,返回本次截取了多少个字节,即原始大小-剩余大小
  }
  
  /**
   * Read from the underlying file into the buffer starting at the given position
   * 参数relativePosition是相对位置,真实的位置是start+relativePosition
   * 
   * channel跳转到relativePosition指定位置,然后开始读数据到buffer中
   * 然后buffer进行flip,好让buffer继续读取已经读到的数据
   */
  def readInto(buffer: ByteBuffer, relativePosition: Int): ByteBuffer = {
    channel.read(buffer, relativePosition + this.start)
    buffer.flip()
    buffer
  }
  
  /**
   * Rename the file that backs this message set
   * @return true iff the rename was successful
   */
  def renameTo(f: File): Boolean = {
    val success = this.file.renameTo(f)
    this.file = f
    success
  }
  
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
