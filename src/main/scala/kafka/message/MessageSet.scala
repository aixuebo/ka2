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

package kafka.message

import java.nio._
import java.nio.channels._

/**
 * Message set helper functions
 * 
 * 该对象表示一个message集合
 */
object MessageSet {

  val MessageSizeLength = 4
  val OffsetLength = 8
  val LogOverhead = MessageSizeLength + OffsetLength
  val Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0))
  
  /**
   * The size of a message set containing the given messages
   */
  def messageSetSize(messages: Iterable[Message]): Int =
    messages.foldLeft(0)(_ + entrySize(_))

  /**
   * The size of a list of messages
   * 获取该message集合所占用的总字节大小
   */
  def messageSetSize(messages: java.util.List[Message]): Int = {
    var size = 0
    val iter = messages.iterator
    while(iter.hasNext) {
      val message = iter.next.asInstanceOf[Message]
      size += entrySize(message)
    }
    size
  }
  
  /**
   * The size of a size-delimited entry in a message set
   * 获取该一个message占用的字节大小
   */
  def entrySize(message: Message): Int = LogOverhead + message.size

}

/**
 * A set of messages with offsets. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. The format of each message is
 * as follows:
 * 8 byte message offset number
 * 4 byte size containing an integer N
 * N message bytes as described in the Message class
 * 
Partition中的每条Message由offset来表示它在这个partition中的偏移量，这个offset不是该Message在partition数据文件中的实际存储位置，而是逻辑上一个值，它唯一确定了partition中的一条Message。
因此，可以认为offset是partition中Message的id。partition中的每条Message包含了以下三个属性：
offset 序号long
MessageSize int类型的message的data字节大小
data 具体data字节

其中offset为long型，MessageSize为int32，表示data有多大，data为message的具体内容。它的格式和Kafka通讯协议中介绍的MessageSet格式是一致。

 该对象表示一个message集合接口
 */
abstract class MessageSet extends Iterable[MessageAndOffset] {

  /** Write the messages in this set to the given channel starting at the given offset byte. 
    * Less than the complete amount may be written, but no more than maxSize can be. The number
    * of bytes written is returned 
    * 将message信息集合中的信息写入到指定的channel中,offset是从哪个字节位置开始写,maxSize表示最多写入多少个字节到channel中
    * return 返回真实写了多少个字节
    **/
  def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Int): Int
  
  /**
   * Provides an iterator over the message/offset pairs in this set
   * 循环迭代每一个message对象
   */
  def iterator: Iterator[MessageAndOffset]
  
  /**
   * Gives the total size of this message set in bytes
   * 该message集合所占用总字节大小
   */
  def sizeInBytes: Int

  /**
   * Print this message set's contents. If the message set has more than 100 messages, just
   * print the first 100.
   */
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(getClass.getSimpleName + "(")
    val iter = this.iterator
    var i = 0
    while(iter.hasNext && i < 100) {
      val message = iter.next
      builder.append(message)
      if(iter.hasNext)
        builder.append(", ")
      i += 1
    }
    if(iter.hasNext)
      builder.append("...")
    builder.append(")")
    builder.toString
  }

}
