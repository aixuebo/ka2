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

/**
 * Some common constants
 * 1秒=1000毫秒(ms) 
1秒=1000000 微秒(μs) 
1秒=1000000000 纳秒(ns) 
 */
object Time {
  val NsPerUs = 1000
  val UsPerMs = 1000
  val MsPerSec = 1000
  val NsPerMs = NsPerUs * UsPerMs //一毫秒转换成微秒
  val NsPerSec = NsPerMs * MsPerSec//一秒转换成微秒
  val UsPerSec = UsPerMs * MsPerSec//一秒转换成微秒
  val SecsPerMin = 60
  val MinsPerHour = 60
  val HoursPerDay = 24
  val SecsPerHour = SecsPerMin * MinsPerHour//一小时转换成秒
  val SecsPerDay = SecsPerHour * HoursPerDay//一天转换成秒
  val MinsPerDay = MinsPerHour * HoursPerDay//一天转换成分
}

/**
 * A mockable interface for time functions
 */
trait Time {
  
  def milliseconds: Long

  def nanoseconds: Long

  def sleep(ms: Long)
}

/**
 * The normal system implementation of time functions
 */
object SystemTime extends Time {
  
  def milliseconds: Long = System.currentTimeMillis
  
  def nanoseconds: Long = System.nanoTime
  
  def sleep(ms: Long): Unit = Thread.sleep(ms)
  
}
