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

package kafka.log

import scala.math._
import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.locks._
import java.util.concurrent.atomic._
import kafka.utils._
import kafka.utils.Utils.inLock
import kafka.common.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * 
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * 
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * 
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an 
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * 
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * 
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the 
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * 
 * The frequency of entries is up to the user of this class.
 * 
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal 
 * storage format.
 * 
 * 表示一个索引文件
 * 存储两个int,
 * 第一个int是在segment中第几个message,如果要计算全局占第几个message,需要用该参数+baseOffset即可
 * 第二个int表示在该segment文件的哪个字节位置存储该message
 * 
 * @param file 表示索引文件  备注:Log.indexFilename方法,产生一个文件dir/00000000000000000001.index
 * @param baseOffset 代表该log的segment文件第一个message在整个topic-partition的位置,这个位置是绝对位置,是10000,就是第10000个message,该值基本上与文件名对应的值相同
 * @param maxIndexSize 表示索引文件的最大字节数
 * 
 */
class OffsetIndex(@volatile var file: File, val baseOffset: Long, val maxIndexSize: Int = -1) extends Logging {
  
  private val lock = new ReentrantLock
  
  /* initialize the memory mapping for this index 将索引文件映射到内存中*/
  private var mmap: MappedByteBuffer = 
    {
      val newlyCreated = file.createNewFile()//新创举一个该文件
      val raf = new RandomAccessFile(file, "rw")
      try {
        /* pre-allocate the file if necessary */
        if(newlyCreated) {
          if(maxIndexSize < 8)
            throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
          raf.setLength(roundToExactMultiple(maxIndexSize, 8))//设置文件的最大字节数,返回第一个参数/8 最近接的整数,例如 67返回的是64,即返回该索引文件最多允许到哪个位置结束,将length之后所有的字节都删除掉
        }
          
        /* memory-map the file */
        val len = raf.length()//返回该文件的总长度
        val idx = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, len)//对文件的全部内容进行内存映射读取,返回的是MappedByteBuffer对象,相当于ByteBuffer使用
          
        /* set the position in the index for the next entry */
        if(newlyCreated)//因为先创建的文件,因此设置position=0,表示从0开始插入数据
          idx.position(0)
        else
          // if this is a pre-existing index, assume it is all valid and set position to last entry
          idx.position(roundToExactMultiple(idx.limit, 8))//从最后一个8的倍数位置开始,以后从该位置开始插入数据,即重新设置position位置,设置到limit接近的位置,下一次写进来的可以继续追加写入
        idx
      } finally {
        Utils.swallow(raf.close())//关闭文件流,返回内存映射buffer对象即可
      }
    }
  
  /* the number of eight-byte entries currently in the index 当前索引位置是第几个message索引位置,因为每个元素占用8个字节位置 */
  private var size = new AtomicInteger(mmap.position / 8)
  
  /**
   * The maximum number of eight-byte entries this index can hold
   * 当前该索引文件中一共能容纳多少个messgae
   */
  @volatile
  var maxEntries = mmap.limit / 8
  
  /* the last offset in the index 
   * 该索引文件中最后一个索引在全局中的序号
   **/
  var lastOffset = readLastEntry.offset

  //加载索引文件xxxx,当前已经存放了多少个message信息了,最多该文件的字节偏移量是xxx,最后一个索引位置是第几个message的序号,当前的位置xxx
  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(file.getAbsolutePath, maxEntries, maxIndexSize, entries(), lastOffset, mmap.position))

  /**
   * The last entry in the index
   * 获取最后一个索引文件对应的OffsetPosition,即最后一个索引文件存储的message序号以及在log文件中的字节偏移量
   */
  def readLastEntry(): OffsetPosition = {
    inLock(lock) {
      size.get match {//size表示一共几个message
        case 0 => OffsetPosition(baseOffset, 0)//说明该日志中还没有存储信息,因此该message的全局log位置就是baseOffset,在该logSegment中的位置是0
        case s => OffsetPosition(baseOffset + relativeOffset(this.mmap, s-1), physical(this.mmap, s-1)) //全局位置是baseOffset+在logSegment中的位置,
      }
    }
  }

  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and it's corresponding physical file position.
   * 
   * @param targetOffset The offset to look up.
   * 
   * @return The offset found and the corresponding file position for this offset. 
   * If the target offset is smaller than the least entry in the index (or the index is empty),
   * the pair (baseOffset, 0) is returned.
   * 返回值是该目标下表在该文件下具体的位置
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, targetOffset)
      if(slot == -1)//说明没查询到
        OffsetPosition(baseOffset, 0)
      else
        OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot))//说明查询到了,则序号就是baseOffset+第N个索引对应是第几个message序号
      }
  }
  
  /**
   * Find the slot in which the largest offset less than or equal to the given
   * target offset is stored.
   * 
   * @param idx The index buffer
   * @param targetOffset The offset to look for 该targetOffset是在全局中第几个message
   * 
   * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
   * 在该文件中查询编号是targetOffset的message,
   * 没有查询到,则返回-1
   * 如果查询到,则返回<=targetOffset的是第几个索引差
   */
  private def indexSlotFor(idx: ByteBuffer, targetOffset: Long): Int = {
    // we only store the difference from the base offset so calculate that 先获取该segment中的相对位置
    val relOffset = targetOffset - baseOffset//首先做差,因为该索引文件中存储的不是真正的message序号
    
    // check if the index is empty 说明该索引中没有元素,因此返回-1,找不到的意思
    if(entries == 0) //没有查询到,则返回-1
      return -1
    
    // check if the target offset is smaller than the least offset 获取该索引文件第0个message的序号差,如果该差假设是100,但是要查找的是80,说明压根就不再这个文件里面,这个文件存储的都是>=100的序号
    //因此返回-1
    if(relativeOffset(idx, 0) > relOffset)//
      return -1
      
    // binary search for the entry 二分法查找最近的是哪个message序号
    var lo = 0
    var hi = entries-1
    while(lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = relativeOffset(idx, mid)
      if(found == relOffset)
        return mid
      else if(found < relOffset)
        lo = mid
      else
        hi = mid - 1
    }
    lo
  }
  
  /* return the nth offset relative to the base offset 获取该segment中第n个索引对应在全局索引的序号*/
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8) //获取的是相对的offset的值
  
  /* return the nth physical position 获取该segment中第n个索引对应在segment的log文件中的位置*/
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8 + 4) //获取的是posotion的值
  
  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   * 获取该索引文件中第n个索引的信息
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if(n >= entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, entries))
      val idx = mmap.duplicate
      OffsetPosition(relativeOffset(idx, n), physical(idx, n))
    }
  }
  
  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   * @param offset 在全部文档中该message属于第几个,注意不是在segment里,而是partition的全局中 
   * @param position 在该segment的哪个节点位置开始插入该offset信息
   */
  def append(offset: Long, position: Int) {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + size + ").") //确保容器不能满
      if (size.get == 0 || offset > lastOffset) {
        debug("Adding index entry %d => %d to %s.".format(offset, position, file.getName))
        this.mmap.putInt((offset - baseOffset).toInt)
        this.mmap.putInt(position)
        this.size.incrementAndGet()
        this.lastOffset = offset
        require(entries * 8 == mmap.position, entries + " entries but file position in index is " + mmap.position + ".")
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, entries, lastOffset, file.getAbsolutePath))
      }
    }
  }
  
  /**
   * True iff there are no more slots available in this index
   * 是否索引文件已经写满了
   */
  def isFull: Boolean = entries >= this.maxEntries
  
  /**
   * Truncate the entire index, deleting all entries
   * 将全部索引信息都删除,但是文件保留,仅仅删除内容
   */
  def truncate() = truncateToEntries(0)
  
  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   * 只是截短到offset序号位置,该序号是全局的message序号
   */
  def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, offset)//搜索该位置对应的是第几个

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries = 
        if(slot < 0)
          0//说明该索引文件不包含该序号message,则删除全部数据
        else if(relativeOffset(idx, slot) == offset - baseOffset)//正好就是这个要删除的序号对应的message
          slot
        else
          slot + 1//说明不是正好是对应的message序号
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   * 删除索引,仅仅保留前entries个索引
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      this.size.set(entries)//先设置索引个数
      mmap.position(this.size.get * 8)//设置索引文件的最后一个位置
      this.lastOffset = readLastEntry.offset//设置最后一个索引文件对应的OffsetPosition
    }
  }
  
  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   * 将文件大小正好设置到容纳当前索引文件的大小位置
   */
  def trimToValidSize() {
    inLock(lock) {
      resize(entries * 8)
    }
  }

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   * 扩容,也可以缩小,不过缩小的话好像该方法不对,因为position的位置设置有问题,因此该方法只能用于扩容,
   * 参数表示要扩容到该大小的文件
   */
  def resize(newSize: Int) {
    inLock(lock) {
      val raf = new RandomAccessFile(file, "rws")//重新读取该文件,
      val roundedNewSize = roundToExactMultiple(newSize, 8)//设置要扩容的文件大小
      val position = this.mmap.position//暂时存储当前已经记录到什么位置了
      
      /* Windows won't let us modify the file length while the file is mmapped :-( */
      if(Os.isWindows)
        forceUnmap(this.mmap)
      try {
        raf.setLength(roundedNewSize)//将文件大小设置成新扩容后的文件大小
        this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)//重新映射内存
        this.maxEntries = this.mmap.limit / 8//设置总共允许容纳多少个message
        this.mmap.position(position)//将原来的position位置重新赋值回来
      } finally {
        Utils.swallow(raf.close())//关闭随机文件流
      }
    }
  }
  
  /**
   * Forcefully free the buffer's mmap. We do this only on windows.
   */
  private def forceUnmap(m: MappedByteBuffer) {
    try {
      if(m.isInstanceOf[sun.nio.ch.DirectBuffer])
        (m.asInstanceOf[sun.nio.ch.DirectBuffer]).cleaner().clean()
    } catch {
      case t: Throwable => warn("Error when freeing index buffer", t)
    }
  }
  
  /**
   * Flush the data in the index to disk
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }
  
  /**
   * Delete this index file
   */
  def delete(): Boolean = {
    info("Deleting index " + this.file.getAbsolutePath)
    this.file.delete()
  }
  
  /** The number of entries in this index 当前有多少个元素*/
  def entries() = size.get
  
  /**
   * The number of bytes actually used by this index
   * 当前索引文件的所占字节大小
   */
  def sizeInBytes() = 8 * entries
  
  /** Close the index */
  def close() {
    trimToValidSize()
  }
  
  /**
   * Rename the file that backs this offset index
   * @return true iff the rename was successful
   */
  def renameTo(f: File): Boolean = {
    val success = this.file.renameTo(f)
    this.file = f
    success
  }
  
  /**
   * Do a basic sanity check on this index to detect obvious problems
   * @throws IllegalArgumentException if any problems are found
   */
  def sanityCheck() {
    require(entries == 0 || lastOffset > baseOffset,
            "Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d"
            .format(file.getAbsolutePath, lastOffset, baseOffset))
      val len = file.length()
      require(len % 8 == 0, 
              "Index file " + file.getName + " is corrupt, found " + len + 
              " bytes which is not positive or not a multiple of 8.")
  }
  
  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundToExactMultiple(67, 8) == 64
   * 返回第一个参数/8 最近接的整数,例如 67返回的是64
   */
  private def roundToExactMultiple(number: Int, factor: Int) = factor * (number / factor)
  
  /**
   * Execute the given function in a lock only if we are running on windows. We do this 
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  private def maybeLock[T](lock: Lock)(fun: => T): T = {
    if(Os.isWindows)
      lock.lock()
    try {
      return fun
    } finally {
      if(Os.isWindows)
        lock.unlock()
    }
  }
}