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
import scala.math._
import kafka.utils._

/**
 * Constants related to messages
 * 指代一个message信息
 * 
 */
object Message {
  
  /**
   * The current offset and size for all the fixed-length fields
   */
  val CrcOffset = 0
  val CrcLength = 4
  val MagicOffset = CrcOffset + CrcLength
  val MagicLength = 1
  val AttributesOffset = MagicOffset + MagicLength
  val AttributesLength = 1
  val KeySizeOffset = AttributesOffset + AttributesLength
  val KeySizeLength = 4
  val KeyOffset = KeySizeOffset + KeySizeLength
  val ValueSizeLength = 4
  
  /** The amount of overhead bytes in a message */
  val MessageOverhead = KeyOffset + ValueSizeLength
  
  /**
   * The minimum valid size for the message header
   */
  val MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength
  
  /**
   * The current "magic" value
   */
  val CurrentMagicValue: Byte = 0

  /**
   * Specifies the mask for the compression code. 3 bits to hold the compression codec.
   * 0 is reserved to indicate no compression
   */
  val CompressionCodeMask: Int = 0x07

  /**
   * Compression code for uncompressed messages
   */
  val NoCompression: Int = 0

}

/**
 * A message. The format of an N byte message is the following:
 * 格式:
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 2 currently
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 * 4. 4 byte key length, containing length K
 * 5. K byte key
 * 6. 4 byte payload length, containing length V
 * 7. V byte payload
 *
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.
 */
class Message(val buffer: ByteBuffer) {
  
  import kafka.message.Message._
  
  /**
   * A constructor to create a Message
   * @param bytes The payload of the message 装着message信息的字节数组
   * @param compressionCodec The compression codec used on the contents of the message (if any)
   * @param key The key of the message (null, if none) key对应的字节数组
   * @param payloadOffset The offset into the payload array used to extract payload 表示value需要从bytes字节数组的哪个位置开始获取 
   * @param payloadSize The size of the payload to use 表示value需要从bytes参数中获取的长度
   * 将该message信息写入到buffer中,而buffer的大小是算出来后创建的
   */
  def this(bytes: Array[Byte], 
           key: Array[Byte],            
           codec: CompressionCodec, 
           payloadOffset: Int, 
           payloadSize: Int) = {
    this(ByteBuffer.allocate(Message.CrcLength + 
                             Message.MagicLength + 
                             Message.AttributesLength + 
                             Message.KeySizeLength + 
                             (if(key == null) 0 else key.length) + 
                             Message.ValueSizeLength + 
                             (if(bytes == null) 0 
                              else if(payloadSize >= 0) payloadSize 
                              else bytes.length - payloadOffset)))
    // skip crc, we will fill that in at the end
    buffer.position(MagicOffset)
    buffer.put(CurrentMagicValue)
    var attributes: Byte = 0
    if (codec.codec > 0)
      attributes =  (attributes | (CompressionCodeMask & codec.codec)).toByte
    buffer.put(attributes)
    if(key == null) {
      buffer.putInt(-1)
    } else {
      buffer.putInt(key.length)
      buffer.put(key, 0, key.length)
    }
    val size = if(bytes == null) -1
               else if(payloadSize >= 0) payloadSize 
               else bytes.length - payloadOffset
    buffer.putInt(size)
    if(bytes != null)
      buffer.put(bytes, payloadOffset, size)
    buffer.rewind()//将buffer的position设置到0的位置,下面要在该位置写入校验和数据
    
    // now compute the checksum and fill it in 在校验和的位置写入校验和
    Utils.writeUnsignedInt(buffer, CrcOffset, computeChecksum)
  }
  
  def this(bytes: Array[Byte], key: Array[Byte], codec: CompressionCodec) = 
    this(bytes = bytes, key = key, codec = codec, payloadOffset = 0, payloadSize = -1)
  
  def this(bytes: Array[Byte], codec: CompressionCodec) = 
    this(bytes = bytes, key = null, codec = codec)
  
  def this(bytes: Array[Byte], key: Array[Byte]) = 
    this(bytes = bytes, key = key, codec = NoCompressionCodec)
    
  def this(bytes: Array[Byte]) = 
    this(bytes = bytes, key = null, codec = NoCompressionCodec)
    
  /**
   * Compute the checksum of the message from the message contents
   * 计算buffer的校验和
   */
  def computeChecksum(): Long = 
    Utils.crc32(buffer.array, buffer.arrayOffset + MagicOffset,  buffer.limit - MagicOffset)
  
  /**
   * Retrieve the previously computed CRC for this message
   * 读取buffer的校验和
   */
  def checksum: Long = Utils.readUnsignedInt(buffer, CrcOffset)
  
    /**
   * Returns true if the crc stored with the message matches the crc computed off the message contents
   * 校验读取的校验和 与 计算的校验和是否相同
   */
  def isValid: Boolean = checksum == computeChecksum
  
  /**
   * Throw an InvalidMessageException if isValid is false for this message
   * 确保校验和有效
   */
  def ensureValid() {
    if(!isValid)
      throw new InvalidMessageException("Message is corrupt (stored crc = " + checksum + ", computed crc = " + computeChecksum() + ")")
  }
  
  /**
   * The complete serialized size of this message in bytes (including crc, header attributes, etc)
   * 该buffer有用信息的字节长度
   */
  def size: Int = buffer.limit
  
  /**
   * The length of the key in bytes
   * key的字节长度
   */
  def keySize: Int = buffer.getInt(Message.KeySizeOffset)
  
  /**
   * Does the message have a key?
   * 是否存在的key
   */
  def hasKey: Boolean = keySize >= 0
  
  /**
   * The position where the payload size is stored
   * value的字节长度在buffer中的位置
   */
  private def payloadSizeOffset = Message.KeyOffset + max(0, keySize)
  
  /**
   * The length of the message value in bytes
   * 获取value的字节长度
   */
  def payloadSize: Int = buffer.getInt(payloadSizeOffset)
  
  /**
   * Is the payload of this message null
   * 是否有value值,true表示没有value
   */
  def isNull(): Boolean = payloadSize < 0
  
  /**
   * The magic version of this message
   * 获取magic字节
   */
  def magic: Byte = buffer.get(MagicOffset)
  
  /**
   * The attributes stored with this message
   * 获取attributes字节
   */
  def attributes: Byte = buffer.get(AttributesOffset)
  
  /**
   * The compression codec used with this message
   * 获取该message的压缩类型
   */
  def compressionCodec: CompressionCodec = 
    CompressionCodec.getCompressionCodec(buffer.get(AttributesOffset) & CompressionCodeMask)
  
  /**
   * A ByteBuffer containing the content of the message
   * 获取value对应的ByteBuffer
   */
  def payload: ByteBuffer = sliceDelimited(payloadSizeOffset)
  
  /**
   * A ByteBuffer containing the message key
   * 获取key对应的ByteBuffer
   */
  def key: ByteBuffer = sliceDelimited(KeySizeOffset)
  
  /**
   * Read a size-delimited byte buffer starting at the given offset
   * 对buffer进行复制,产生一个新的buffer,而该新的buffer与老buffer共用同一块内存
   */
  private def sliceDelimited(start: Int): ByteBuffer = {
    val size = buffer.getInt(start)
    if(size < 0) {
      null
    } else {
      var b = buffer.duplicate//复制该buffer
      b.position(start + 4)//设置新buffer的position位置
      b = b.slice()//对该新的buffer进行分片
      b.limit(size)
      b.rewind
      b
    }
  }

  override def toString(): String = 
    "Message(magic = %d, attributes = %d, crc = %d, key = %s, payload = %s)".format(magic, attributes, checksum, key, payload)
  
  override def equals(any: Any): Boolean = {
    any match {
      case that: Message => this.buffer.equals(that.buffer)
      case _ => false
    }
  }
  
  override def hashCode(): Int = buffer.hashCode
  
}
