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

import java.io.InputStream
import java.nio.ByteBuffer

//在ByteBuffer中读取字节
class ByteBufferBackedInputStream(buffer:ByteBuffer) extends InputStream {
  
  //返回读取的字节是什么
  override def read():Int  = {
    buffer.hasRemaining match {//buffer.hasRemaining = true表示有字节可以读取
      case true =>
        (buffer.get() & 0xFF)
      case false => -1
    }
  }

  //返回读取了多少个字节
  override def read(bytes:Array[Byte], off:Int, len:Int):Int = {
    buffer.hasRemaining match {//buffer.hasRemaining = true表示有字节可以读取
      case true =>
        // Read only what's left
        val realLen = math.min(len, buffer.remaining())//计算真正可以读取的字节数量
        buffer.get(bytes, off, realLen)//真正去读取字节,读取后填充到bytes里面
        realLen
      case false => -1
    }
  }
}
