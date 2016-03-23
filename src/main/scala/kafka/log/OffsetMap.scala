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

import java.util.Arrays
import java.security.MessageDigest
import java.nio.ByteBuffer
import kafka.utils._

/**
 * 就是一个map,存储的value是一个long类型的偏移量
 */
trait OffsetMap {
  def slots: Int //理论上可以存储多少个元素
  def put(key: ByteBuffer, offset: Long) //存储一个键值对
  def get(key: ByteBuffer): Long //通过key获取value
  def clear()
  def size: Int //获取有多少个元素
  def utilization: Double = size.toDouble / slots //当前使用率
}

/**
 * An hash table used for deduplicating the log. This hash table uses a cryptographicly secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
 * @param memory The amount of memory this map can use 使用空间
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512 hash算法
 */
@nonthreadsafe
class SkimpyOffsetMap(val memory: Int, val hashAlgorithm: String = "MD5") extends OffsetMap {
  private val bytes = ByteBuffer.allocate(memory)//就分配这么多空间存储map内存映射
  
  /* the hash algorithm instance to use, defualt is MD5 */
  private val digest = MessageDigest.getInstance(hashAlgorithm)
  
  /* the number of bytes for this hash algorithm hash算法产生的字节长度*/
  private val hashSize = digest.getDigestLength
  
  /* create some hash buffers to avoid reallocating each time 创建两个buffer,避免再分配内存*/
  private val hash1 = new Array[Byte](hashSize)
  private val hash2 = new Array[Byte](hashSize)
  
  /* number of entries put into the map */
  private var entries = 0 //容器装有多少个元素
  
  /* number of lookups on the map */
  private var lookups = 0L //记录查找次数,即调用put和get的时候都会累加1
  
  /* the number of probes for all lookups 每次put和get的时候,一旦遇见冲突了,每次解决冲突的时候都会累加1,因此该值比>=lookups */
  private var probes = 0L
  
  /**
   * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
   * 每一个entry需要多少字节,因为一个entry包含key的hash作为key,long类型的偏移量作为value,因此占用hashSize + 8个字节
   */
  val bytesPerEntry = hashSize + 8
  
  /**
   * The maximum number of entries this map can contain
   * 该内存允许有多少个坑
   */
  val slots: Int = (memory / bytesPerEntry).toInt
  
  /**
   * Associate this offset to the given key.分配一个key和偏移量
   * @param key The key
   * @param offset The offset
   */
  override def put(key: ByteBuffer, offset: Long) {
    require(entries < slots, "Attempt to add a new entry to a full offset map.") //必须有坑
    lookups += 1
    hashInto(key, hash1) //对key进行hash,然后将hash后的值存储到hash1中
    // probe until we find the first empty slot
    var attempt = 0
    var pos = positionOf(hash1, attempt)  
    while(!isEmpty(pos)) {//无限循环,直到找到没有使用过的坑结束
      bytes.position(pos)//定位到pos的位置
      bytes.get(hash2)//获取当前该位置存储的值
      if(Arrays.equals(hash1, hash2)) {//如果要添加的key已经存在,则更新对应的偏移量value即可返回
        // we found an existing entry, overwrite it and return (size does not change)
        bytes.putLong(offset)
        return
      }
      attempt += 1//说明没找到对应的key,则继续尝试查找
      pos = positionOf(hash1, attempt)
    }
    // found an empty slot, update it--size grows by 1
    bytes.position(pos)//定位到没有元素的坑的偏移量
    bytes.put(hash1)//在该位置设置key的hash值
    bytes.putLong(offset) //存储value
    entries += 1 //容器装有多少个元素
  }
  
  /**
   * Check that there is no entry at the given position
   * true表示该position位置是没有元素的
   */
  private def isEmpty(position: Int): Boolean = 
    bytes.getLong(position) == 0 && bytes.getLong(position + 8) == 0 && bytes.getLong(position + 16) == 0
  
  /**
   * Get the offset associated with this key.
   * @param key The key
   * @return The offset associated with this key or -1 if the key is not found
   * 返回key对应的偏移量value,如果找不到则返回-1
   */
  override def get(key: ByteBuffer): Long = {
    lookups += 1
    hashInto(key, hash1) //对key进行hash,然后将hash后的值存储到hash1中
    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    var attempt = 0
    var pos = 0
    do {
      pos = positionOf(hash1, attempt)//定位到该位置
      bytes.position(pos)//定位到pos位置
      if(isEmpty(pos))//说明没找到,则返回-1
        return -1L
      bytes.get(hash2)//将pos位置的内容读取到hash2中
      attempt += 1
    } while(!Arrays.equals(hash1, hash2)) //如果key没找到,则继续查找
    bytes.getLong() //说明找到了,并且也获取了key的hash到hash2了,因此这行代码表示返回对应的value
  }
  
  /**
   * Change the salt used for key hashing making all existing keys unfindable.
   * Doesn't actually zero out the array.
   */
  override def clear() {
    this.entries = 0
    this.lookups = 0L
    this.probes = 0L
    //将数组内容都转换成0
    Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit, 0.toByte)
  }
  
  /**
   * The number of entries put into the map (note that not all may remain)
   * 返回该map填充了多少个元素
   */
  override def size: Int = entries
  
  /**
   * The rate of collisions in the lookups
   * 碰撞率
   * this.probes - this.lookups = 冲突的次数
   * 冲突次数/查询或者put总数,等于冲突率
   */
  def collisionRate: Double = 
    (this.probes - this.lookups) / this.lookups.toDouble
  
  /**
   * Calculate the ith probe position. We first try reading successive integers from the hash itself
   * then if all of those fail we degrade to linear probing.
   * @param hash The hash of the key to find the position for 要存储的hash值
   * @param attempt The ith probe 可能放在第一个坑上
   * @return The byte offset in the buffer at which the ith probing for the given hash would reside 返回该坑的字节偏移量位置
   * 
   * md5的话,hash内容是16个字节的值
   */
  private def positionOf(hash: Array[Byte], attempt: Int): Int = {
    //从hash的math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4)位置开始读取四个字节,组成一个int
    /**
math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4)
math.min(attempt, hashSize - 4)) 从0到hashSize - 4之间,比如hashSize=16,则返回0-12之间的整数,即attempt到12之间
math.max(0, attempt - hashSize + 4) 如果后面的得到负数,那结果就是0,如果后面得到的是整数,则结果就是后面的值,
因此该得到的结果是 0 到attempt - hashSize + 4之间的整数,如果hashSize=16,则返回0到attempt-12之间的值,只有attempt大于13的时候才返回有效的值
     */
    val probe = Utils.readInt(hash, math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4) //可能是第几个slots
    val slot = Utils.abs(probe) % slots //计算是哪个坑
    this.probes += 1
    slot * bytesPerEntry //找到该坑所在的字节位置
  }
  
  /**
   * The offset at which we have stored the given key
   * @param key The key to hash
   * @param buffer The buffer to store the hash into
   * 对key进行hash,然后将hash后的值存储到buffer中
   */
  private def hashInto(key: ByteBuffer, buffer: Array[Byte]) {
    key.mark()
    digest.update(key)//对key进行hash算法运算成字节数组
    key.reset()
    digest.digest(buffer, 0, hashSize)//将key进行hash后的值,存储到buffer中,并且digest算法被清空,下次重新进行update的时候不冲突
  }
  
}