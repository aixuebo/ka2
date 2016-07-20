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

package kafka.utils

import java.io._
import java.nio._
import charset.Charset
import java.nio.channels._
import java.util.concurrent.locks.{ReadWriteLock, Lock}
import java.lang.management._
import javax.management._
import scala.collection._
import scala.collection.mutable
import java.util.Properties
import kafka.common.KafkaException
import kafka.common.KafkaStorageException


/**
 * General helper functions!
 * 
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc. 
 * 
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
object Utils extends Logging {

  /**
   * Wrap the given function in a java.lang.Runnable
   * @param fun A function
   * @return A Runnable that just executes the function
   * 产生一个runnable类
   */
  def runnable(fun: => Unit): Runnable =
    new Runnable {
      def run() = fun
    }

  /**
   * Create a daemon thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   * 产生一个后台执行的线程
   */
  def daemonThread(runnable: Runnable): Thread =
    newThread(runnable, true)

  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   * 产生一个后台执行的线程
   */
  def daemonThread(name: String, runnable: Runnable): Thread = 
    newThread(name, runnable, true)
  
  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param fun The runction to execute in the thread
   * @return The unstarted thread
   * 产生一个后台执行的线程,并且线程的实现体为无返回值的fun函数
   */
  def daemonThread(name: String, fun: () => Unit): Thread = 
    daemonThread(name, runnable(fun))
  
  /**
   * Create a new thread
   * @param name The name of the thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   * 产生一个线程
   */
  def newThread(name: String, runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable, name) 
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      } 
    })
    thread
  }
   
  /**
   * Create a new thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   * 产生一个线程
   */
  def newThread(runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      }
    })
    thread
  }
  
  /**
   * Read the given byte buffer into a byte array
   * 读取buffer数组一定字符,读取到一个Array[Byte]中
   */
  def readBytes(buffer: ByteBuffer): Array[Byte] = readBytes(buffer, 0, buffer.limit)

  /**
   * Read a byte array from the given offset and size in the buffer
   * 读取buffer数组一定字符,读取到一个Array[Byte]中
   */
  def readBytes(buffer: ByteBuffer, offset: Int, size: Int): Array[Byte] = {
    val dest = new Array[Byte](size)
    if(buffer.hasArray) {
      System.arraycopy(buffer.array, buffer.arrayOffset() + offset, dest, 0, size)
    } else {
      buffer.mark()
      buffer.get(dest)
      buffer.reset()
    }
    dest
  }

  /**
   * Read a properties file from the given path
   * @param filename The path of the file to read ,properties文件的全路径
   * 读取properties文件
   */
   def loadProps(filename: String): Properties = {
     val props = new Properties()
     var propStream: InputStream = null
     try {
       propStream = new FileInputStream(filename)
       props.load(propStream)
     } finally {
       if(propStream != null)
         propStream.close
     }
     props
   }

  /**
   * Open a channel for the given file
   * 根据参数返回一个是否支持写的流
   */
  def openChannel(file: File, mutable: Boolean): FileChannel = {
    if(mutable)
      new RandomAccessFile(file, "rw").getChannel()
    else
      new FileInputStream(file).getChannel()
  }
  
  /**
   * Do the given action and log any exceptions thrown without rethrowing them
   * @param log The log method to use for logging. E.g. logger.warn
   * @param action The action to execute
   * 执行无返回值的action函数,然后如果出现异常,将日志输出到log中,log参数是两个
   * 函数名次为咽下,即出了异常也没什么,打印日志即可
   */
  def swallow(log: (Object, Throwable) => Unit, action: => Unit) {
    try {
      action
    } catch {
      case e: Throwable => log(e.getMessage(), e)
    }
  }
  
  /**
   * Test if two byte buffers are equal. In this case equality means having
   * the same bytes from the current position to the limit
   * 比较两个ByteBuffer的内容是否相同
   */
  def equal(b1: ByteBuffer, b2: ByteBuffer): Boolean = {
    // two byte buffers are equal if their position is the same,
    // their remaining bytes are the same, and their contents are the same
    if(b1.position != b2.position)
      return false
    if(b1.remaining != b2.remaining)
      return false
    for(i <- 0 until b1.remaining)
      if(b1.get(i) != b2.get(i))
        return false
    return true
  }
  
  /**
   * Translate the given buffer into a string
   * @param buffer The buffer to translate
   * @param encoding The encoding to use in translating bytes to characters
   * 将ByteBuffer中的字节数组,根据编码转换成字符串
   */
  def readString(buffer: ByteBuffer, encoding: String = Charset.defaultCharset.toString): String = {
    val bytes = new Array[Byte](buffer.remaining)
    buffer.get(bytes)
    new String(bytes, encoding)
  }
  
  /**
   * Print an error message and shutdown the JVM
   * @param message The error message
   * 打印错误信息,并且停止程序运行
   */
  def croak(message: String) {
    System.err.println(message)
    System.exit(1)
  }
  
  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   * @param file The root file at which to begin deleting
   * 删除一个文件,支持递归删除文件夹
   */
  def rm(file: String): Unit = rm(new File(file))
  
  /**
   * Recursively delete the list of files/directories and any subfiles (if any exist)
   * @param a sequence of files to be deleted
   * 删除一组文件集合,支持递归删除文件夹
   */
  def rm(files: Seq[String]): Unit = files.map(f => rm(new File(f)))
  
  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   * @param file The root file at which to begin deleting
   * 删除一个文件,支持递归删除文件夹
   */
  def rm(file: File) {
	  if(file == null) {
	    return
	  } else if(file.isDirectory) {
	    val files = file.listFiles()
	    if(files != null) {
	      for(f <- files)
	        rm(f)
	    }
	    file.delete()
	  } else {
	    file.delete()
	  }
  }
  
  /**
   * Register the given mbean with the platform mbean server,
   * unregistering any mbean that was there before. Note,
   * this method will not throw an exception if the registration
   * fails (since there is nothing you can do and it isn't fatal),
   * instead it just returns false indicating the registration failed.
   * @param mbean The object to register as an mbean
   * @param name The name to register this mbean with
   * @return true if the registration succeeded
   */
  def registerMBean(mbean: Object, name: String): Boolean = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer()
      mbs synchronized {
        val objName = new ObjectName(name)
        if(mbs.isRegistered(objName))
          mbs.unregisterMBean(objName)
        mbs.registerMBean(mbean, objName)
        true
      }
    } catch {
      case e: Exception => {
        error("Failed to register Mbean " + name, e)
        false
      }
    }
  }
  
  /**
   * Unregister the mbean with the given name, if there is one registered
   * @param name The mbean name to unregister
   */
  def unregisterMBean(name: String) {
    val mbs = ManagementFactory.getPlatformMBeanServer()
    mbs synchronized {
      val objName = new ObjectName(name)
      if(mbs.isRegistered(objName))
        mbs.unregisterMBean(objName)
    }
  }
  
  /**
   * Read an unsigned integer from the current position in the buffer, 
   * incrementing the position by 4 bytes
   * @param buffer The buffer to read from
   * @return The integer read, as a long to avoid signedness
   */
  def readUnsignedInt(buffer: ByteBuffer): Long = 
    buffer.getInt() & 0xffffffffL
  
  /**
   * Read an unsigned integer from the given position without modifying the buffers
   * position
   * @param buffer the buffer to read from
   * @param index the index from which to read the integer
   * @return The integer read, as a long to avoid signedness
   */
  def readUnsignedInt(buffer: ByteBuffer, index: Int): Long = 
    buffer.getInt(index) & 0xffffffffL
  
  /**
   * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
   * @param buffer The buffer to write to
   * @param value The value to write
   */
  def writetUnsignedInt(buffer: ByteBuffer, value: Long): Unit = 
    buffer.putInt((value & 0xffffffffL).asInstanceOf[Int])
  
  /**
   * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
   * @param buffer The buffer to write to
   * @param index The position in the buffer at which to begin writing
   * @param value The value to write
   */
  def writeUnsignedInt(buffer: ByteBuffer, index: Int, value: Long): Unit = 
    buffer.putInt(index, (value & 0xffffffffL).asInstanceOf[Int])
  
  /**
   * Compute the CRC32 of the byte array
   * @param bytes The array to compute the checksum for
   * @return The CRC32
   * 对bytes字节数组进行计算校验码,返回该校验码
   */
  def crc32(bytes: Array[Byte]): Long = crc32(bytes, 0, bytes.length)
  
  /**
   * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
   * @param bytes The bytes to checksum
   * @param offset the offset at which to begin checksumming
   * @param size the number of bytes to checksum
   * @return The CRC32
   * 对bytes字节数组进行计算校验码,返回该校验码
   */
  def crc32(bytes: Array[Byte], offset: Int, size: Int): Long = {
    val crc = new Crc32()
    crc.update(bytes, offset, size)
    crc.getValue()
  }
  
  /**
   * Compute the hash code for the given items
   */
  def hashcode(as: Any*): Int = {
    if(as == null)
      return 0
    var h = 1
    var i = 0
    while(i < as.length) {
      if(as(i) != null) {
        h = 31 * h + as(i).hashCode
        i += 1
      }
    }
    return h
  }
  
  /**
   * Group the given values by keys extracted with the given function
   * 1.循环每一个value
   * 2.每一个value作为参数传到f函数中,返回k
   * 3.按照k进行分组,k就是key,k相同的value作为集合返回
   */
  def groupby[K,V](vals: Iterable[V], f: V => K): Map[K,List[V]] = {
    val m = new mutable.HashMap[K, List[V]]
    for(v <- vals) {
      val k = f(v)
      m.get(k) match {
        case Some(l: List[V]) => m.put(k, v :: l)
        case None => m.put(k, List(v))
      }
    } 
    m
  }
  
  /**
   * Read some bytes into the provided buffer, and return the number of bytes read. If the 
   * channel has been closed or we get -1 on the read for any reason, throw an EOFException
   * 将ReadableByteChannel的数据写入到buffer中,返回读取了多少个字节
   */
  def read(channel: ReadableByteChannel, buffer: ByteBuffer): Int = {
    channel.read(buffer) match {
      case -1 => throw new EOFException("Received -1 when reading from channel, socket has likely been closed.")
      case n: Int => n
    }
  } 
  
  /**
   * Throw an exception if the given value is null, else return it. You can use this like:
   * val myValue = Utils.notNull(expressionThatShouldntBeNull)
   * 确保v不是null即可
   */
  def notNull[V](v: V) = {
    if(v == null)
      throw new KafkaException("Value cannot be null.")
    else
      v
  }

  /**
   * Get the stack trace from an exception as a string
   * 打印堆栈信息,并且返回该堆栈信息
   */
  def stackTrace(e: Throwable): String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString()
  }

  /**
   * This method gets comma separated values which contains key,value pairs and returns a map of
   * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
   * 格式key1:val1, key2:val2 转化成Map
   */
  def parseCsvMap(str: String): Map[String, String] = {
    val map = new mutable.HashMap[String, String]
    if("".equals(str))
      return map    
    val keyVals = str.split("\\s*,\\s*").map(s => s.split("\\s*:\\s*"))
    keyVals.map(pair => (pair(0), pair(1))).toMap
  }
  
  /**
   * Parse a comma separated string into a sequence of strings.
   * Whitespace surrounding the comma will be removed.
   * 按照空格拆分成集合,过滤掉空的元素
   * 例如aaa,bbb,ccc 返回三个集合
   */
  def parseCsvList(csvList: String): Seq[String] = {
    if(csvList == null || csvList.isEmpty)
      Seq.empty[String]
    else {
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""))
    }
  }

  /**
   * Create an instance of the class with the given class name
   * 创建一个class实例,参数是class全路径以及构造函数参数集合
   */
  def createObject[T<:AnyRef](className: String, args: AnyRef*): T = {
    val klass = Class.forName(className).asInstanceOf[Class[T]]
    val constructor = klass.getConstructor(args.map(_.getClass): _*)
    constructor.newInstance(args: _*).asInstanceOf[T]
  }

  /**
   * Is the given string null or empty ("")?
   * null或者""则返回true
   */
  def nullOrEmpty(s: String): Boolean = s == null || s.equals("")

  /**
   * Create a circular (looping) iterator over a collection.
   * @param coll An iterable over the underlying collection.
   * @return A circular iterator over the collection.
   *  for里面有;表示for 循环里面套for循环
   */
  def circularIterator[T](coll: Iterable[T]) = {
    val stream: Stream[T] =
      for (forever <- Stream.continually(1); t <- coll) yield t
    stream.iterator
  }

  /**
   * Attempt to read a file as a string
   * 将一个文件的内容返回成字符串
   */
  def readFileAsString(path: String, charset: Charset = Charset.defaultCharset()): String = {
    val stream = new FileInputStream(new File(path))
    try {
      val fc = stream.getChannel()
      val bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size())
      charset.decode(bb).toString()
    }
    finally {
      stream.close()
    }
  }
  
  /**
   * Get the absolute value of the given number. If the number is Int.MinValue return 0.
   * This is different from java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
   * 获取绝对值方法
   */
  def abs(n: Int) = if(n == Integer.MIN_VALUE) 0 else math.abs(n)

  /**
   * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
   * 确保文件s以oldSuffix结尾
   * 例如xxx.oldSuffix 最后返回xxx.newSuffix
   */
  def replaceSuffix(s: String, oldSuffix: String, newSuffix: String): String = {
    if(!s.endsWith(oldSuffix))
      throw new IllegalArgumentException("Expected string to end with '%s' but string is '%s'".format(oldSuffix, s))
    s.substring(0, s.length - oldSuffix.length) + newSuffix
  }

  /**
   * Create a file with the given path
   * @param path The path to create
   * @throws KafkaStorageException If the file create fails
   * @return The created file
   * 创建一个文件
   */
  def createFile(path: String): File = {
    val f = new File(path)
    val created = f.createNewFile()
    if(!created)
      throw new KafkaStorageException("Failed to create file %s.".format(path))
    f
  }
  
  /**
   * Turn a properties map into a string
   * 将Properties信息转换成String
   */
  def asString(props: Properties): String = {
    val writer = new StringWriter()
    props.store(writer, "")
    writer.toString
  }
  
  /**
   * Read some properties with the given default values
   * 读取默认的Properties配置信息,通过s可以更改和添加新的配置信息,最终返回新的配置信息
   */
  def readProps(s: String, defaults: Properties): Properties = {
    val reader = new StringReader(s)
    val props = new Properties(defaults)
    props.load(reader)
    props
  }
  
  /**
   * Read a big-endian integer from a byte array
   * 从bytes的offset位置开始读取四个字节,组成一个int
   */
  def readInt(bytes: Array[Byte], offset: Int): Int = {
    ((bytes(offset) & 0xFF) << 24) |
    ((bytes(offset + 1) & 0xFF) << 16) |
    ((bytes(offset + 2) & 0xFF) << 8) |
    (bytes(offset + 3) & 0xFF)
  }
  
  /**
   * Execute the given function inside the lock
   * 在锁中执行fun函数,函数fun是无参数,又返回值的函数
   */
  def inLock[T](lock: Lock)(fun: => T): T = {
    lock.lock()
    try {
      fun
    } finally {
      lock.unlock()
    }
  }

  //在读锁中操作fun函数
  def inReadLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.readLock)(fun)

  //在写锁中操作fun函数
  def inWriteLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.writeLock)(fun)


  //JSON strings need to be escaped based on ECMA-404 standard http://json.org
  def JSONEscapeString (s : String) : String = {
    s.map {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '/'  => "\\/"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      /* We'll unicode escape any control characters. These include:
       * 0x0 -> 0x1f  : ASCII Control (C0 Control Codes)
       * 0x7f         : ASCII DELETE
       * 0x80 -> 0x9f : C1 Control Codes
       *
       * Per RFC4627, section 2.5, we're not technically required to
       * encode the C1 codes, but we do to be safe.
       */
      case c if ((c >= '\u0000' && c <= '\u001f') || (c >= '\u007f' && c <= '\u009f')) => "\\u%04x".format(c: Int)
      case c => c
    }.mkString 
  }

  /**
   * Returns a list of duplicated items
   * 找到集合中有重复的key的集合
   * 比如参数集合是 1、2、2、4、5、4,则返回2和4
   *
   * 逻辑:
   * 按照每一个key进行分组,分组后是每一个key有多少个元素,然后过滤掉元素数量>1的,就是重复的,返回重复的key集合即可
   */
  def duplicates[T](s: Traversable[T]): Iterable[T] = {
    s.groupBy(identity)
      .map{ case (k,l) => (k,l.size)}
      .filter{ case (k,l) => (l > 1) }
      .keys
  }
}
