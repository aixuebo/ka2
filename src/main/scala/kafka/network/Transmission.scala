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
import kafka.utils.Logging
import kafka.common.KafkaException

/**
 * Represents a stateful transfer of data to or from the network
 * 代表一个有状态的传输任务
 */
private[network] trait Transmission extends Logging {
  
  def complete: Boolean//是否已经完成该传输
  
  //期望是不完成,如果不完成则不抛异常
  protected def expectIncomplete(): Unit = {
    if(complete)
      throw new KafkaException("This operation cannot be completed on a complete request.")
  }

  //期望是完成,如果完成则不抛异常
  protected def expectComplete(): Unit = {
    if(!complete)
      throw new KafkaException("This operation cannot be completed on an incomplete request.")
  }
  
}

/**
 * A transmission that is being received from a channel
 * 从渠道接收到一个传输任务
 */
trait Receive extends Transmission {
  
  def buffer: ByteBuffer
  
  //ReadableByteChannel 表示数据源,从该数据源读取数据,一般读取到指定的ByteBuffer中,返回本次读取了多少个字节
  def readFrom(channel: ReadableByteChannel): Int
  
  //接收数据,ReadableByteChannel 表示数据源,从ReadableByteChannel中读取数据,一般读取到指定的ByteBuffer中
  //阻塞读取,直到readChannel中的数据都读取完成之后才停止
  def readCompletely(channel: ReadableByteChannel): Int = {
    var totalRead = 0
    while(!complete) {
      val read = readFrom(channel)//从channel中读取了多少个字节
      trace(read + " bytes read.")
      totalRead += read
    }
    totalRead
  }
  
}

/**
 * A transmission that is being sent out to the channel
 * 向一个channel中写入数据任务
 */
trait Send extends Transmission {
    
  //向GatheringByteChannel中写入数据,GatheringByteChannel作为目的地,返回写入多少个字节
  def writeTo(channel: GatheringByteChannel): Int

  //向GatheringByteChannel中写入数据,返回写入多少个字节数据
  //注意:阻塞写入,直到RequestOrResponse中数据都写入完成为止才能推出
  def writeCompletely(channel: GatheringByteChannel): Int = {
    var totalWritten = 0
    while(!complete) {//只要不完成,就一直去写下去
      val written = writeTo(channel)//向渠道中写入数据
      trace(written + " bytes written.")
      totalWritten += written
    }
    totalWritten
  }
    
}

/**
 * A set of composite sends, sent one after another
 * 一个send,一个send的写入到输出流GatheringByteChannel目的地中
 */
abstract class MultiSend[S <: Send](val sends: List[S]) extends Send {
  val expectedBytesToWrite: Int//期望写入的字节数
  private var current = sends//等待发送的数据源集合信息
  var totalWritten = 0//已经写入多少个字节到指定GatheringByteChannel目的地中

  /**
   *  This method continues to write to the socket buffer till an incomplete
   *  write happens. On an incomplete write, it returns to the caller to give it
   *  a chance to schedule other work till the buffered write completes.
   *  
   *  
   *  向GatheringByteChannel中写入数据
   *  返回本次调用发送了多少个字节
   */
  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete//校验,确保目前还没有完成
    var totalWrittenPerCall = 0//本次调用,发送了多少个字节
    var sendComplete: Boolean = false
    do {
      val written = current.head.writeTo(channel)//获取send集合的第一个send,将该send信息写入到channel中
      totalWritten += written//统计写入了多少个字节
      totalWrittenPerCall += written//本次发送了多少个字节
      sendComplete = current.head.complete
      if(sendComplete)
        current = current.tail
    } while (!complete && sendComplete)
    trace("Bytes written as part of multisend call : " + totalWrittenPerCall +  "Total bytes written so far : " + totalWritten + "Expected bytes to write : " + expectedBytesToWrite)
    totalWrittenPerCall
  }
  
  def complete: Boolean = {
    if (current == Nil) {
      if (totalWritten != expectedBytesToWrite)
        //打印错误日志,说写入的字节和期望的不同,但是
        error("mismatch in sending bytes over socket; expected: " + expectedBytesToWrite + " actual: " + totalWritten)
      true
    } else {
      false
    }
  }
}
