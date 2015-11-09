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
 package kafka.api

import java.nio._
import kafka.common._

/**
 * Helper functions specific to parsing or serializing requests and responses
 * 请求序列化和返回的反序列化
 */
object ApiUtils {
  
  val ProtocolEncoding = "UTF-8"

    /**
   * Read size prefixed string where the size is stored as a 2 byte short.
   * @param buffer The buffer to read from
   * 读取一个short可以容纳的字符串字节数组
   * 详细查看writeShortString方法
   * 
   * 将字节数组转换成字符串
   */
  def readShortString(buffer: ByteBuffer): String = {
    val size: Int = buffer.getShort()
    if(size < 0)
      return null
    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    new String(bytes, ProtocolEncoding)
  }
  
  /**
   * Write a size prefixed string where the size is stored as a 2 byte short
   * @param buffer The buffer to write to
   * @param string The string to write
   * 当string进行utf-8转码后,长度依然小于short时,则将其存储到buffer中,格式:字节数组长度、字节数组
   * 将字符串转化成字节数组
   */
  def writeShortString(buffer: ByteBuffer, string: String) {
    if(string == null) {
      buffer.putShort(-1)
    } else {
      val encodedString = string.getBytes(ProtocolEncoding)
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
      } else {
        buffer.putShort(encodedString.length.asInstanceOf[Short])
        buffer.put(encodedString)
      }
    }
  }
  
  /**
   * Return size of a size prefixed string where the size is stored as a 2 byte short
   * @param string The string to write
   * 
   * 计算该参数string所占用的字节长度,因为需要一个2个字节的short存储字符串长度,因此最后返回的是string的字节长度+2
   */
  def shortStringLength(string: String): Int = {
    if(string == null) {
      2
    } else {
      val encodedString = string.getBytes(ProtocolEncoding)
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
      } else {
        2 + encodedString.length
      }
    }
  }
  
  /**
   * Read an integer out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   * 从buffer中读取一个int,校验该int在range开区间内,即() 该int值是name的属性值
   */
  def readIntInRange(buffer: ByteBuffer, name: String, range: (Int, Int)): Int = {
    val value = buffer.getInt
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
    else value
  }

  /**
   * Read a short out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   * 从buffer中读取一个short,校验该short在range开区间内,即() 该short值是name的属性值
   */
  def readShortInRange(buffer: ByteBuffer, name: String, range: (Short, Short)): Short = {
    val value = buffer.getShort
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
    else value
  }

  /**
   * Read a long out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   * 从buffer中读取一个long,校验该long在range开区间内,即() 该long值是name的属性值
   */
  def readLongInRange(buffer: ByteBuffer, name: String, range: (Long, Long)): Long = {
    val value = buffer.getLong
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
    else value
  }
  
}
