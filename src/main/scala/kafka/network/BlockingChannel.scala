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

import java.net.InetSocketAddress
import java.nio.channels._
import kafka.utils.{nonthreadsafe, Logging}
import kafka.api.RequestOrResponse


object BlockingChannel{
  val UseDefaultBufferSize = -1
}

/**
 *  A simple blocking channel with timeouts correctly enabled.
 * 参数readBufferSize和writeBufferSize、readTimeoutMs都是socket参数,接收和写入的参数
 * 
 * 该类属于客户端,即该类向host:port所在服务器建立连接请求,并且收发信息的阻塞方式客户端
 */
@nonthreadsafe
class BlockingChannel( val host: String, 
                       val port: Int, 
                       val readBufferSize: Int, 
                       val writeBufferSize: Int, 
                       val readTimeoutMs: Int ) extends Logging {
  private var connected = false
  private var channel: SocketChannel = null//客户端向服务器发送连接请求的渠道
  private var readChannel: ReadableByteChannel = null//客户端的输入流,即客户端从服务器接收到数据存在这里面
  private var writeChannel: GatheringByteChannel = null//客户端的输出流,即客户端要发送的数据存在这里面
  private val lock = new Object()
  private val connectTimeoutMs = readTimeoutMs

  //创建一个SocketChannel,设置好socket参数,然后向host、port发送请求,因此该过程说明该对象是客户端
  def connect() = lock synchronized  {
    if(!connected) {
      try {
        //建立连接请求
        channel = SocketChannel.open()
        if(readBufferSize > 0)
          channel.socket.setReceiveBufferSize(readBufferSize)
        if(writeBufferSize > 0)
          channel.socket.setSendBufferSize(writeBufferSize)
        channel.configureBlocking(true)
        channel.socket.setSoTimeout(readTimeoutMs)
        channel.socket.setKeepAlive(true)
        channel.socket.setTcpNoDelay(true)
        channel.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs)

        //该请求就是写入渠道
        writeChannel = channel
        //服务器传回的数据
        readChannel = Channels.newChannel(channel.socket().getInputStream)
        connected = true
        //打印日志,说明连接完成
        // settings may not match what we requested above
        val msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d), connectTimeoutMs = %d."
        debug(msg.format(channel.socket.getSoTimeout,
                         readTimeoutMs,
                         channel.socket.getReceiveBufferSize, 
                         readBufferSize,
                         channel.socket.getSendBufferSize,
                         writeBufferSize,
                         connectTimeoutMs))

      } catch {
        case e: Throwable => disconnect()
      }
    }
  }
  
  //断开连接,关闭资源,设置connected = false
  def disconnect() = lock synchronized {
    if(channel != null) {
      swallow(channel.close())
      swallow(channel.socket.close())
      channel = null
      writeChannel = null
    }
    // closing the main socket channel *should* close the read channel
    // but let's do it to be sure.
    if(readChannel != null) {
      swallow(readChannel.close())
      readChannel = null
    }
    connected = false
  }

  def isConnected = connected//是否连接成功
  
  //将RequestOrResponse中的数据 写入到writeChannel中
  def send(request: RequestOrResponse):Int = {
    if(!connected)
      throw new ClosedChannelException()

    val send = new BoundedByteBufferSend(request)
    send.writeCompletely(writeChannel)//阻塞写入,直到RequestOrResponse中数据都写入完成为止才能推出
  }
  
  //阻塞读取,直到readChannel中的数据都读取完成之后才停止
  def receive(): Receive = {
    if(!connected)
      throw new ClosedChannelException()

    val response = new BoundedByteBufferReceive()
    response.readCompletely(readChannel)

    response
  }

}
