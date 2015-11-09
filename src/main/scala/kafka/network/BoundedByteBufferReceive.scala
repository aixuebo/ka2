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

package kafka.network

import java.nio._
import java.nio.channels._
import kafka.utils._

/**
 * Represents a communication between the client and server
 * 从channel中接收数据到contentBuffer中
 * 
 * @param maxSize 表示接收的字节最多不允许超过该字节数
 */
@nonthreadsafe
private[kafka] class BoundedByteBufferReceive(val maxSize: Int) extends Receive with Logging {
  
  private val sizeBuffer = ByteBuffer.allocate(4)//本次请求中有多少个字节,即contentBuffer的buffer需要多少个字节
  private var contentBuffer: ByteBuffer = null//读取的内容缓存在该buffer中
  
  def this() = this(Int.MaxValue)
  
  var complete: Boolean = false//是否读取完成
  
  /**
   * Get the content buffer for this transmission
   * 首先去报已经完成,然后才获取该完成后的buffer对象
   */
  def buffer: ByteBuffer = {
    expectComplete()
    contentBuffer
  }
  
  /**
   * Read the bytes in this response from the given channel
   * 从给定的ReadableByteChannel中读取数据,返回读取多少个字节
   */
  def readFrom(channel: ReadableByteChannel): Int = {
    //首先确保没完成,才可能读数据
    expectIncomplete()
    var read = 0//读取的字节数
    // have we read the request size yet? 说明还没有读取请求的字节大小
    if(sizeBuffer.remaining > 0)
      read += Utils.read(channel, sizeBuffer)//ByteBuffer的大小是4个字节,因此正好可以从channel中读取,返回读取了多少个字节
    // have we allocated the request buffer yet?
    if(contentBuffer == null && !sizeBuffer.hasRemaining) {//如果contentBuffer没有被初始化,即第一次使用,并且已经接收到了需要多少个字节,则进行初始化contentBuffer
      sizeBuffer.rewind()//重置到0的位置,获取int值,表示接收的文件是多少个字节
      val size = sizeBuffer.getInt()
      if(size <= 0)
        throw new InvalidRequestException("%d is not a valid request size.".format(size))
      if(size > maxSize)
        throw new InvalidRequestException("Request of length %d is not valid, it is larger than the maximum size of %d bytes.".format(size, maxSize))
      //对buffer进行初始化
      contentBuffer = byteBufferAllocate(size)
    }
    // if we have a buffer read some stuff into it
    if(contentBuffer != null) {//如果contentBuffer已经存在了,则将channel数据写入到buffer中
      read = Utils.read(channel, contentBuffer)//将ReadableByteChannel的数据写入到buffer中,返回读取了多少个字节
      // did we get everything?
      if(!contentBuffer.hasRemaining) {//如果contentBuffer没有数据了,则说明数据已经读取完成,因此设置complete=true,并且读取的buffer的位置设置到0位置
        contentBuffer.rewind()
        complete = true
      }
    }
    read
  }

  //创建ByteBuffer缓冲区
  private def byteBufferAllocate(size: Int): ByteBuffer = {
    var buffer: ByteBuffer = null
    try {
      buffer = ByteBuffer.allocate(size)
    } catch {
      case e: OutOfMemoryError =>
        error("OOME with size " + size, e)
        throw e
      case e2: Throwable =>
        throw e2
    }
    buffer
  }
}
