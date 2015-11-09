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
import kafka.api.RequestOrResponse

/**
 * 从buffer中数据发送到channel中
 */
@nonthreadsafe
private[kafka] class BoundedByteBufferSend(val buffer: ByteBuffer) extends Send {
  
  private var sizeBuffer = ByteBuffer.allocate(4)//用int类型的4个字节记录buffer中有多少个字节可用

  // Avoid possibility of overflow for 2GB-4 byte buffer 校验buffer要发送的数据不能超过int的范围,不然没办法使用sizeBuffer标示有多少个字节要发送了
  if(buffer.remaining > Int.MaxValue - sizeBuffer.limit)
    throw new IllegalStateException("Attempt to create a bounded buffer of " + buffer.remaining + " bytes, but the maximum " +
                                       "allowable size for a bounded buffer is " + (Int.MaxValue - sizeBuffer.limit) + ".")    
  sizeBuffer.putInt(buffer.limit)//设置buffer中有多少个字节,该值就是buffer的大小,初始化的时候就设置好了buffer的大小了
  sizeBuffer.rewind()//将sizeBuffer重置到0位置,方便下次读取sizeBuffer的值

  var complete: Boolean = false

  //创建size大小的ByteBuffer用于存储待发送的字节数组
  def this(size: Int) = this(ByteBuffer.allocate(size))
  
  def this(request: RequestOrResponse) = {
    this(request.sizeInBytes + (if(request.requestId != None) 2 else 0))//初始化buffer的大小,如果有request.requestId,则占用两个字节的short
    request.requestId match {
      case Some(requestId) =>
        buffer.putShort(requestId)//先存储两个字节的short,表示哪种类型的RequestOrResponse
      case None =>
    }

    //将RequestOrResponse的具体内容写入到buffer中
    request.writeTo(buffer)
    //重置buffer的位置为0,方便后续从头处理buffer
    buffer.rewind()
  }

  //将bufer数据写入到channel渠道中.返回写入了多少个字节
  //要想写入数据,说明尚未完成
  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()//校验,保证尚未完成
    var written = channel.write(Array(sizeBuffer, buffer))//将两个ByteBuffer数组发送到channel中,第一个记录第二个buffer中有多少个字节
    // if we are done, mark it off
    if(!buffer.hasRemaining)//如果第二个buffer没有数据了,则说明完成发送了,设置complete=true
      complete = true    
    written.asInstanceOf[Int]
  }
    
}
