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
 * 格式:
 * 4个字节的crc校验和
 * 1个字节的magic魔
 * 1个字节的属性长度,即该属性设置的是该message的加密方式属性
 * 4个字节代表key字节长度
 * key的内容
 * 4个字节表示value的长度
 * value的内容
 */
object Message {
  
  /**
   * The current offset and size for all the fixed-length fields
   */
  val CrcOffset = 0//crc校验和的开始位置
  val CrcLength = 4//crc校验和所占字节数
  val MagicOffset = CrcOffset + CrcLength//magic魔的开始位置 4
  val MagicLength = 1//magic魔的长度
  val AttributesOffset = MagicOffset + MagicLength//属性的开始位置 5
  val AttributesLength = 1//属性的长度
  val KeySizeOffset = AttributesOffset + AttributesLength//key的开始位置 6
  val KeySizeLength = 4//存储key的长度的字节大小,int类型
  val KeyOffset = KeySizeOffset + KeySizeLength//key的开始位置 10
  val ValueSizeLength = 4//存储value的长度的字节大小,int类型
  
  /** The amount of overhead bytes in a message */
  val MessageOverhead = KeyOffset + ValueSizeLength //14,表示真正message的信息的开始位置
  
  /**
   * The minimum valid size for the message header
   * 最小的一个message也要有这些字符,因为这些字符是头文件的大小
   */
  val MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength //14,表示每一个message信息包含着14个字节头文件
  
  /**
   * The current "magic" value 当前魔版本号
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
 * 构造函数的参数是为一个message预先分配的缓存空间,正好可以存储该message信息
 */
class Message(val buffer: ByteBuffer) {
  
  import kafka.message.Message._
  
  /**
   * A constructor to create a Message
   * @param bytes The payload of the message 装着message信息的字节数组
   * @param codec The compression codec used on the contents of the message (if any)
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
                              /**
                              * 预先设置一个message信息所占空间,即14个头文件字节+key的字节+value的字节,
                              * 注意
                              * value的字节数如果payloadSize>=0.表示value只要这些长度的值
                              * value的字节数为value总长度 - payloadOffset,即从该payloadOffset偏移量位置开始才是真正的value
                              * 
                              */
    // skip crc, we will fill that in at the end 跳过crc,最后才会写入该crc
    buffer.position(MagicOffset)
    buffer.put(CurrentMagicValue)//写入魔
    var attributes: Byte = 0 //设置属性,即该属性表示message的加密方式
    if (codec.codec > 0)
      attributes =  (attributes | (CompressionCodeMask & codec.codec)).toByte
    buffer.put(attributes) //设置文件的加密方式属性
    if(key == null) {//如果没有key,则设置key为-1
      buffer.putInt(-1)
    } else {//否则设置key的字节长度+字节信息
      buffer.putInt(key.length)
      buffer.put(key, 0, key.length)
    }
    val size = if(bytes == null) -1
               else if(payloadSize >= 0) payloadSize 
               else bytes.length - payloadOffset
    buffer.putInt(size)//设置value的字节长度
    if(bytes != null)
      buffer.put(bytes, payloadOffset, size) //设置value的字节内容
    buffer.rewind()//将buffer的position设置到0的位置,下面要在该位置写入校验和数据,因此设置到0的位置,用于从0开始计算校验和数据
    
    // now compute the checksum and fill it in 在校验和的位置写入校验和
    Utils.writeUnsignedInt(buffer, CrcOffset, computeChecksum)//进行校验和计算以及填写到校验和的位置
  }
  
  //key和value都不是null
  def this(bytes: Array[Byte], key: Array[Byte], codec: CompressionCodec) = 
    this(bytes = bytes, key = key, codec = codec, payloadOffset = 0, payloadSize = -1)
  
    //key默认为null,仅包含value
  def this(bytes: Array[Byte], codec: CompressionCodec) = 
    this(bytes = bytes, key = null, codec = codec)
  
    //key和value都不是null
  def this(bytes: Array[Byte], key: Array[Byte]) = 
    this(bytes = bytes, key = key, codec = NoCompressionCodec)
    
    //key默认为null,仅包含value
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
   * 从存储key长度的位置读取int值,表示key的长度
   */
  def keySize: Int = buffer.getInt(Message.KeySizeOffset)
  
  /**
   * Does the message have a key?
   * true表示key不是null
   */
  def hasKey: Boolean = keySize >= 0
  
  /**
   * The position where the payload size is stored
   * value的字节长度在buffer中的位置
   * 即key开始位置+key长度占用的4个字节+key所占用的所有字节,即value的开始位置
   */
  private def payloadSizeOffset = Message.KeyOffset + max(0, keySize)
  
  /**
   * The length of the message value in bytes
   * 获取value的字节长度
   * 从value的开始位置读取,返回的就是value所占的长度
   */
  def payloadSize: Int = buffer.getInt(payloadSizeOffset)
  
  /**
   * Is the payload of this message null
   * 是否有value值,true表示没有value,即value的长度<0表示没有value
   */
  def isNull(): Boolean = payloadSize < 0
  
  /**
   * The magic version of this message
   * 获取magic字节
   */
  def magic: Byte = buffer.get(MagicOffset)
  
  /**
   * The attributes stored with this message
   * 获取attributes字节,即获取该message的压缩方式属性
   */
  def attributes: Byte = buffer.get(AttributesOffset)
  
  /**
   * The compression codec used with this message
   * 通过压缩方式属性,获取该message的压缩类型
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
    val size = buffer.getInt(start)//比如start是key的开始位置,则这行代码可以获取key所占字节大小
    if(size < 0) {
      null
    } else {
      var b = buffer.duplicate//复制该buffer
      b.position(start + 4)//设置新buffer的position位置,将key的大小位置过滤掉,剩下的就是key的具体内容所占字节了
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
