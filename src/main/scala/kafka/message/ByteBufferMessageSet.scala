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

import kafka.utils.Logging
import java.nio.ByteBuffer
import java.nio.channels._
import java.io.{InputStream, ByteArrayOutputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicLong
import kafka.utils.IteratorTemplate

/**
 * bytebuffer存储一组message信息
 */
object ByteBufferMessageSet {
  
  private def create(offsetCounter: AtomicLong, compressionCodec: CompressionCodec, messages: Message*): ByteBuffer = {
    if(messages.size == 0) {//没有message则返回空的ByteBuffer
      MessageSet.Empty.buffer
    } else if(compressionCodec == NoCompressionCodec) {//不压缩
      val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for(message <- messages)
        writeMessage(buffer, message, offsetCounter.getAndIncrement)
      buffer.rewind()//将buffer的position设置到0的位置
      buffer
    } else {//需要压缩
      val byteArrayStream = new ByteArrayOutputStream(MessageSet.messageSetSize(messages))//根据消息字节大小,设置输出流
      val output = new DataOutputStream(CompressionFactory(compressionCodec, byteArrayStream))//根据压缩算法,生成输出流包装容器
      var offset = -1L
      try {
        /**
       * 将message的信息写入到out输出流中,offset表示该message的排序
       * 格式:
       * 8个字节的offset
       * 4个字节的message消息的字节长度
       * 写入若干个字节,将message信息都写入到out输出流中
         */
        for(message <- messages) {
          offset = offsetCounter.getAndIncrement
          output.writeLong(offset)
          output.writeInt(message.size)
          output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
        }
      } finally {
        output.close()
      }
      val bytes = byteArrayStream.toByteArray//压缩后的字节数组
      val message = new Message(bytes, compressionCodec)//用压缩后的字节数组组成新的Message对象
      val buffer = ByteBuffer.allocate(message.size + MessageSet.LogOverhead)//分配新的ByteBuffer对象,并且返回
      writeMessage(buffer, message, offset)
      buffer.rewind()
      buffer
    }
  }
  
  //解压缩成ByteBufferMessageSet对象
  def decompress(message: Message): ByteBufferMessageSet = {
    val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream//最终输出流
    val inputStream: InputStream = new ByteBufferBackedInputStream(message.payload)//读取压缩的输入流
    val intermediateBuffer = new Array[Byte](1024)//存储中间的临时信息
    val compressed = CompressionFactory(message.compressionCodec, inputStream)//对压缩的输入流进行转换,解码操作的流
    try {
      //从压缩流中读取还原后的数据,写入到outputStream中,其中dataRead表示读取了多少个字节,读取的信息都存储到临时buffer中--intermediateBuffer
      Stream.continually(compressed.read(intermediateBuffer)).takeWhile(_ > 0).foreach { dataRead =>
        outputStream.write(intermediateBuffer, 0, dataRead)
      }
    } finally {
      compressed.close()
    }
    val outputBuffer = ByteBuffer.allocate(outputStream.size)
    outputBuffer.put(outputStream.toByteArray)
    outputBuffer.rewind
    new ByteBufferMessageSet(outputBuffer)
  }
    
  /**
   * 将message的信息写入到buffer中,offset表示该message的排序
   * 格式:
   * 8个字节的offset
   * 4个字节的message消息的字节长度
   * 写入若干个字节,将message信息都写入到buffer中
   */
  private[kafka] def writeMessage(buffer: ByteBuffer, message: Message, offset: Long) {
    buffer.putLong(offset)
    buffer.putInt(message.size)
    buffer.put(message.buffer)
    message.buffer.rewind()
  }
}

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 * 
 */
class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet with Logging {
  private var shallowValidByteCount = -1

  //Consumers消费者使用该构造方法
  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(ByteBufferMessageSet.create(new AtomicLong(0), compressionCodec, messages:_*))
  }
  
  //Consumers消费者使用该构造方法
  def this(compressionCodec: CompressionCodec, offsetCounter: AtomicLong, messages: Message*) {
    this(ByteBufferMessageSet.create(offsetCounter, compressionCodec, messages:_*))
  }

  //producter生产者使用该构造方法
  def this(messages: Message*) {
    this(NoCompressionCodec, new AtomicLong(0), messages: _*)
  }

  def getBuffer = buffer

  private def shallowValidBytes: Int = {
    if(shallowValidByteCount < 0) {
      var bytes = 0
      val iter = this.internalIterator(true)
      while(iter.hasNext) {
        val messageAndOffset = iter.next
        bytes += MessageSet.entrySize(messageAndOffset.message)
      }
      this.shallowValidByteCount = bytes
    }
    shallowValidByteCount
  }
  
  /** Write the messages in this set to the given channel 
   *  将GatheringByteChannel的信息从offer开始写入到buffer中,写入size个
   **/
  def writeTo(channel: GatheringByteChannel, offset: Long, size: Int): Int = {
    // Ignore offset and size from input. We just want to write the whole buffer to the channel.
    buffer.mark()
    var written = 0
    while(written < sizeInBytes)
      written += channel.write(buffer)
    buffer.reset()
    written
  }

  /** default iterator that iterates over decompressed messages */
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(true)

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. 
   *  isShallow = true表示浅遍历,仅仅遍历第一个级别的message信息
   *  **/
  private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var topIter = buffer.slice()
      var innerIter: Iterator[MessageAndOffset] = null

      def innerDone():Boolean = (innerIter == null || !innerIter.hasNext)

      //真正执行遍历方法
      def makeNextOuter: MessageAndOffset = {
        // if there isn't at least an offset and size, we are done
        if (topIter.remaining < 12)
          return allDone()
        val offset = topIter.getLong()//该消息的偏移量
        val size = topIter.getInt()//该消息所占用字节长度
        if(size < Message.MinHeaderSize)//每一个message长度必须超过该值
          throw new InvalidMessageException("Message found with corrupt size (" + size + ")")
        
        // we have an incomplete message
        if(topIter.remaining < size)
          return allDone()
          
        // read the current message and check correctness
        val message = topIter.slice()//分片,产生一个新的buffer,与老buffer共用同一组底层buffer
        message.limit(size)//设置limit
        topIter.position(topIter.position + size)//topIter跳过该数据
        val newMessage = new Message(message)//获取新的message信息

        if(isShallow) {//浅遍历返回即可
          new MessageAndOffset(newMessage, offset)
        } else {//深度遍历
          newMessage.compressionCodec match {
            case NoCompressionCodec =>
              innerIter = null
              new MessageAndOffset(newMessage, offset)
            case _ =>
              innerIter = ByteBufferMessageSet.decompress(newMessage).internalIterator()
              if(!innerIter.hasNext)
                innerIter = null
              makeNext()
          }
        }
      }

      override def makeNext(): MessageAndOffset = {
        if(isShallow){
          makeNextOuter
        } else {
          if(innerDone())
            makeNextOuter
          else
            innerIter.next
        }
      }
      
    }
  }
  
  /**
   * Update the offsets for this message set. This method attempts to do an in-place conversion
   * if there is no compression, but otherwise recopies the messages
   */
  private[kafka] def assignOffsets(offsetCounter: AtomicLong, codec: CompressionCodec): ByteBufferMessageSet = {
    if(codec == NoCompressionCodec) {
      // do an in-place conversion
      var position = 0
      buffer.mark()
      while(position < sizeInBytes - MessageSet.LogOverhead) {
        buffer.position(position)
        buffer.putLong(offsetCounter.getAndIncrement())
        position += MessageSet.LogOverhead + buffer.getInt()
      }
      buffer.reset()
      this
    } else {
      // messages are compressed, crack open the messageset and recompress with correct offset
      val messages = this.internalIterator(isShallow = false).map(_.message)
      new ByteBufferMessageSet(compressionCodec = codec, offsetCounter = offsetCounter, messages = messages.toBuffer:_*)
    }
  }
 

  /**
   * The total number of bytes in this message set, including any partial trailing messages
   */
  def sizeInBytes: Int = buffer.limit
  
  /**
   * The total number of bytes in this message set not including any partial, trailing messages
   */
  def validBytes: Int = shallowValidBytes

  /**
   * Two message sets are equal if their respective byte buffers are equal
   */
  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet => 
        buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = buffer.hashCode

}
