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

import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.net._
import java.io._
import java.nio.channels._

import scala.collection._

import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import com.yammer.metrics.core.{Gauge, Meter}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 * 服务器端对象 ,每一个broker对象都要启动一个该服务端对象
 */
class SocketServer(val brokerId: Int,//该服务器节点标示,标示哪台服务器
                   val host: String,//该服务器的host,用于客户端连接该服务器的host:port
                   val port: Int,//该服务器的port,用于客户端连接该服务器的host:port
                   val numProcessorThreads: Int,//线程数
                   val maxQueuedRequests: Int,//最大允许队列中存储多少个request请求
                   val sendBufferSize: Int,//该socket服务器发送信息的缓存,属于socket设置
                   val recvBufferSize: Int,//该socket服务器接收信息的缓存,属于socket设置
                   val maxRequestSize: Int = Int.MaxValue,//表示接收的字节最多不允许超过该字节数,因为该类表示服务器,因此接收的数据即请求的数据不允许超过该值
                   val maxConnectionsPerIp: Int = Int.MaxValue,//默认的每一个ip能连接的最多连接次数
                   val connectionsMaxIdleMs: Long,//单位是秒
                   val maxConnectionsPerIpOverrides: Map[String, Int] )//key是ip,value是该ip最多能连接的次数,该参数与 maxConnectionsPerIp参数联合使用
                   extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Socket Server on Broker " + brokerId + "], "//表示是哪台节点上的服务端被启动了
  private val time = SystemTime
  //处理队列数量集合
  private val processors = new Array[Processor](numProcessorThreads)
  
  @volatile private var acceptor: Acceptor = null
  
  //存储request队列的对象
  val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)

  /* a meter to track the average free capacity of the network processors */
  private val aggregateIdleMeter = newMeter("NetworkProcessorAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

  /**
   * Start the socket server
   * 开启服务器socket
   */
  def startup() {
    //配置每一个ip对应的允许最大的连接数
    val quotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
    for(i <- 0 until numProcessorThreads) {
      //表示处理线程池中的一个线程对象
      processors(i) = new Processor(i, 
                                    time, 
                                    maxRequestSize, 
                                    aggregateIdleMeter,
                                    newMeter("IdlePercent", "percent", TimeUnit.NANOSECONDS, Map("networkProcessor" -> i.toString)),
                                    numProcessorThreads, 
                                    requestChannel,
                                    quotas,
                                    connectionsMaxIdleMs)
      Utils.newThread("kafka-network-thread-%d-%d".format(port, i), processors(i), false).start()
    }

    newGauge("ResponsesBeingSent", new Gauge[Int] {
      def value = processors.foldLeft(0) { (total, p) => total + p.countInterestOps(SelectionKey.OP_WRITE) }
    })

    // register the processor threads for notification of responses
    requestChannel.addResponseListener((id:Int) => processors(id).wakeup())
   
    // start accepting connections
    this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize, quotas)
    Utils.newThread("kafka-socket-acceptor", acceptor, false).start()
    acceptor.awaitStartup
    info("Started")
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    if(acceptor != null)
      acceptor.shutdown()
    for(processor <- processors)
      processor.shutdown()
    info("Shutdown completed")
  }
}

/**
 * A base class with some helper variables and methods
 * 参数connectionQuotas,表示每一个ip的连接数约束
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  protected val selector = Selector.open();
  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(true)//是否活跃

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    selector.wakeup()
    shutdownLatch.await
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get
  
  /**
   * Wakeup the thread for selection.
   */
  def wakeup() = selector.wakeup()
  
  /**
   * Close the given key and associated socket
   */
  def close(key: SelectionKey) {
    if(key != null) {
      key.attach(null)
      close(key.channel.asInstanceOf[SocketChannel])
      swallowError(key.cancel())
    }
  }
  
  def close(channel: SocketChannel) {
    if(channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
  
  /**
   * Close all open connections
   */
  def closeAll() {
    // removes cancelled keys from selector.keys set
    this.selector.selectNow() 
    val iter = this.selector.keys().iterator()
    while (iter.hasNext) {
      val key = iter.next()
      close(key)
    }
  }

  def countInterestOps(ops: Int): Int = {
    var count = 0
    val it = this.selector.keys().iterator()
    while (it.hasNext) {
      if ((it.next().interestOps() & ops) != 0) {
        count += 1
      }
    }
    count
  }
}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
private[kafka] class Acceptor(val host: String, 
                              val port: Int, 
                              private val processors: Array[Processor],
                              val sendBufferSize: Int, 
                              val recvBufferSize: Int,//该socket服务器接收信息的缓存
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) {
  val serverChannel = openServerSocket(host, port)//开启一个服务器,准备接受请求

  /**
   * Accept loop that checks for new connection attempts
   * 在accept上接收注册
   */
  def run() {
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    startupComplete()
    var currentProcessor = 0//需要第几个处理器去处理该请求
    while(isRunning) {
      val ready = selector.select(500)
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isAcceptable)
               accept(key, processors(currentProcessor))//让第n个处理器处理该请求
            else
               throw new IllegalStateException("Unrecognized key state for acceptor thread.")

            // round robin to the next processor thread 轮训到下一个处理器去处理请求
            currentProcessor = (currentProcessor + 1) % processors.length
          } catch {
            case e: Throwable => error("Error while accepting connection", e)
          }
        }
      }
    }
    debug("Closing server socket and selector.")
    swallowError(serverChannel.close())
    swallowError(selector.close())
    shutdownComplete()
  }
  
  /*
   * Create a server socket to listen for connections on.
   * 在host:port上开启socket,为客户端去连接
   */
  def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress = 
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, port))
    } catch {
      case e: SocketException => 
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostName, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   * 使processor处理器去处理本次接收的请求SelectionKey
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)//记录该ip向服务器建立了一个连接
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)//设置向该客户端发送数据的buffer缓存

      debug("Accepted connection from %s on %s. sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getInetAddress, socketChannel.socket.getLocalSocketAddress,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 * 处理线程池中的一个线程对象
 */
private[kafka] class Processor(val id: Int,//该处理器的序号
                               val time: Time,
                               val maxRequestSize: Int,//表示接收的字节最多不允许超过该字节数,因为该类表示服务器,因此接收的数据即请求的数据不允许超过该值
                               val aggregateIdleMeter: Meter,
                               val idleMeter: Meter,
                               val totalProcessorThreads: Int,//该服务器一共启动了多少个线程数,即第一个参数id属于该线程数之一
                               val requestChannel: RequestChannel,//全局的Request请求队列
                               connectionQuotas: ConnectionQuotas,//连接约束对象
                               val connectionsMaxIdleMs: Long) 
                               extends AbstractServerThread(connectionQuotas) {

  //存储与该处理器进行处理的客户端流SocketChannel集合
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000//将秒转化成微秒
  private var currentTimeNanos = SystemTime.nanoseconds//当前毫秒数
  //记录每一个SelectionKey对应的最近的活跃日期
  private val lruConnections = new util.LinkedHashMap[SelectionKey, Long]
  private var nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos

  override def run() {
    startupComplete()
    while(isRunning) {
      // setup any new connections that have been queued up 从队列中获取一个客户端的请求,然后读取该请求信息
      //读取一个request请求
      configureNewConnections()
      // register any new responses for writing 读取一个response去写数据
      processNewResponses()
      val startSelectTime = SystemTime.nanoseconds
      val ready = selector.select(300)
      currentTimeNanos = SystemTime.nanoseconds
      val idleTime = currentTimeNanos - startSelectTime//空闲时间
      idleMeter.mark(idleTime)//记录空闲时间
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      aggregateIdleMeter.mark(idleTime / totalProcessorThreads)

      trace("Processor id " + id + " selection time = " + idleTime + " ns")
      //根据获取的请求类型,进行读取request数据、向request写入response信息、关闭request的链接
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isReadable)//进行读取request数据
              read(key)
            else if(key.isWritable)//向request写入response信息
              write(key)
            else if(!key.isValid)//关闭request的链接
              close(key)
            else
              throw new IllegalStateException("Unrecognized key state for processor thread.")
          } catch {
            case e: EOFException => {
              info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
              close(key)
            } case e: InvalidRequestException => {
              info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress, e.getMessage))
              close(key)
            } case e: Throwable => {
              error("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error", e)
              close(key)
            }
          }
        }
      }
      maybeCloseOldestConnection
    }
    debug("Closing selector.")
    closeAll()
    swallowError(selector.close())
    shutdownComplete()
  }

  /**
   * Close the given key and associated socket
   * 关闭request的链接
   */
  override def close(key: SelectionKey): Unit = {
    lruConnections.remove(key)
    super.close(key)
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      val key = curr.request.requestKey.asInstanceOf[SelectionKey]
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction => {
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            key.interestOps(SelectionKey.OP_READ)
            key.attach(null)
          }
          case RequestChannel.SendAction => {
            trace("Socket server received response to send, registering for write: " + curr)
            key.interestOps(SelectionKey.OP_WRITE)
            key.attach(curr)
          }
          case RequestChannel.CloseConnectionAction => {
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(key)
          }
          case responseCode => throw new KafkaException("No mapping found for response code " + responseCode)
        }
      } catch {
        case e: CancelledKeyException => {
          debug("Ignoring response for closed socket.")
          close(key)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /**
   * Queue up a new connection for reading
   * 一个socket客户端已经与服务器建立了连接,并且产生了该连接流SocketChannel
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   * 从队列中获取一个客户端的请求,然后读取该请求信息
   */
  private def configureNewConnections() {
    while(newConnections.size() > 0) {
      val channel = newConnections.poll()
      debug("Processor " + id + " listening to new connection from " + channel.socket.getRemoteSocketAddress)
      channel.register(selector, SelectionKey.OP_READ)
    }
  }

  /*
   * Process reads from ready sockets
   * 进行读取request数据
   */
  def read(key: SelectionKey) {
    lruConnections.put(key, currentTimeNanos)
    val socketChannel = channelFor(key)//获取与该SelectionKey对应的SocketChannel客户端与服务器连接流
    var receive = key.attachment.asInstanceOf[Receive]//获取或者绑定一个接收数据的对象
    if(key.attachment == null) {
      receive = new BoundedByteBufferReceive(maxRequestSize)
      key.attach(receive)
    }
    
    //从流中接收数据,返回接收了多少数据
    val read = receive.readFrom(socketChannel)
    //记录日志从哪个客户端接收了多少个字节数据
    val address = socketChannel.socket.getRemoteSocketAddress();
    trace(read + " bytes read from " + address)
    if(read < 0) {//没有数据被读取到,则关闭链接,说明已经读取完了
      close(key)
    } else if(receive.complete) {//接收完成,则将收到的信息进行处理
      val req = RequestChannel.Request(processor = id, requestKey = key, buffer = receive.buffer, startTimeMs = time.milliseconds, remoteAddress = address)
      requestChannel.sendRequest(req)
      key.attach(null)
      // explicitly reset interest ops to not READ, no need to wake up the selector just yet
      key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
    } else {//说明有更多数据要被读,期待下次读取信息
      // more reading to be done
      trace("Did not finish reading, registering for read again on connection " + socketChannel.socket.getRemoteSocketAddress())
      key.interestOps(SelectionKey.OP_READ)
      wakeup()
    }
  }

  /*
   * Process writes to ready sockets
   * 向request写入response信息
   */
  def write(key: SelectionKey) {
    val socketChannel = channelFor(key)//获取与该SelectionKey对应的SocketChannel客户端与服务器连接流
    val response = key.attachment().asInstanceOf[RequestChannel.Response]
    val responseSend = response.responseSend
    if(responseSend == null)
      throw new IllegalStateException("Registered for write interest but no response attached to key.")
    val written = responseSend.writeTo(socketChannel)
    trace(written + " bytes written to " + socketChannel.socket.getRemoteSocketAddress() + " using key " + key)
    if(responseSend.complete) {
      response.request.updateRequestMetrics()
      key.attach(null)
      trace("Finished writing, registering for read on connection " + socketChannel.socket.getRemoteSocketAddress())
      key.interestOps(SelectionKey.OP_READ)
    } else {
      trace("Did not finish writing, registering for write again on connection " + socketChannel.socket.getRemoteSocketAddress())
      key.interestOps(SelectionKey.OP_WRITE)
      wakeup()
    }
  }

  //获取与该SelectionKey对应的SocketChannel客户端与服务器连接流
  private def channelFor(key: SelectionKey) = key.channel().asInstanceOf[SocketChannel]

  //关闭最老的一个连接
  private def maybeCloseOldestConnection {
    if(currentTimeNanos > nextIdleCloseCheckTime) {
      if(lruConnections.isEmpty) {
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos
      } else {
        val oldestConnectionEntry = lruConnections.entrySet.iterator().next()
        val connectionLastActiveTime = oldestConnectionEntry.getValue
        nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos
        if(currentTimeNanos > nextIdleCloseCheckTime) {
          val key: SelectionKey = oldestConnectionEntry.getKey
          trace("About to close the idle connection from " + key.channel.asInstanceOf[SocketChannel].socket.getRemoteSocketAddress
            + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis")
          close(key)
        }
      }
    }
  }

}

//链接的约束对象,用于约束一些信息,不符合要求的不允许连接
/**
 * @param 默认每一个InetAddress最多能允许多少个连接
 * @param overrideQuotas key是InetAddress,value是该InetAddress对应能最多允许的链接数量
 */
class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {
  //配置每一个InetAddress允许最大的连接数,超过该允许值则抛异常.认为连接过多
  private val overrides = overrideQuotas.map(entry => (InetAddress.getByName(entry._1), entry._2))
  
  //key是InetAddress,value是该InetAddress对应的次数
  //该对象存储的真实的每一个InetAddress对应多少个连接
  private val counts = mutable.Map[InetAddress, Int]()
  
  //为该InetAddress增加
  def inc(addr: InetAddress) {
    counts synchronized {
      val count = counts.getOrElse(addr, 0)//先获取该InetAddress对应的int,默认是0
      counts.put(addr, count + 1)//重新设置该InetAddress对应的int,累加1
      val max = overrides.getOrElse(addr, defaultMax)//获取该InetAddress对象允许的最多连接数
      if(count >= max)//超出范围则抛异常
        throw new TooManyConnectionsException(addr, max)
    }
  }
  
  //减少一个InetAddress的链接
  def dec(addr: InetAddress) {
    counts synchronized {
      val count = counts.get(addr).get
      if(count == 1)
        counts.remove(addr)
      else
        counts.put(addr, count - 1)
    }
  }
  
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
