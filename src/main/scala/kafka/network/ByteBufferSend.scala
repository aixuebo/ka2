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
 * 从buffer中数据发送到channel中
 */
@nonthreadsafe
private[kafka] class ByteBufferSend(val buffer: ByteBuffer) extends Send {
  
  var complete: Boolean = false

  //根据size大小创建一个ByteBuffer缓冲区,等待向该缓冲区填写数据
  def this(size: Int) = this(ByteBuffer.allocate(size))
  
  //将bufer数据写入到channel渠道中.返回写入了多少个字节
  //要想写入数据,说明尚未完成
  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()//校验,保证尚未完成
    var written = 0
    written += channel.write(buffer)
    if(!buffer.hasRemaining)//如果buffer中没有数据了,则说明发送完了,因此complete设置成true
      complete = true
    written
  }
    
}
