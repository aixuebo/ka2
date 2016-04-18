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

import java.util.concurrent._
import scala.math._

/**
 * 很多DelayQueue的例子。就拿上海的SB会来说明，很多国家地区的开馆时间不同。你很早就来到园区，然后急急忙忙地跑到一些心仪的馆区，发现有些还没开，你吃了闭门羹。 
仔细研究DelayQueue，你会发现它其实就是一个PriorityQueue的封装（按照delay时间排序），里面的元素都实现了Delayed接口，相关操作需要判断延时时间是否到了
 */
class DelayedItem[T](val item: T, delay: Long, unit: TimeUnit) extends Delayed with Logging {

  val createdMs = SystemTime.milliseconds //创建DelayedItem的时间
  
  //表示总共需要延迟多长时间
  val delayMs = {
    val given = unit.toMillis(delay)//根据delay和TimeUnit单位,计算延迟多少毫秒
    if (given < 0 || (createdMs + given) < 0) (Long.MaxValue - createdMs)
    else given
  }

  //构造函数,添加默认时间单位为毫秒
  def this(item: T, delayMs: Long) = 
    this(item, delayMs, TimeUnit.MILLISECONDS)

  /**
   * The remaining delay time
   * 还需要延迟多少TimeUnit单位时间
   */
  def getDelay(unit: TimeUnit): Long = {
    val elapsedMs = (SystemTime.milliseconds - createdMs) //已经延迟了多长时间
    unit.convert(max(delayMs - elapsedMs, 0), TimeUnit.MILLISECONDS) //返回剩余延迟多长时间
  }
   
  //按照延迟后的最后时间排序
  def compareTo(d: Delayed): Int = {
    val delayed = d.asInstanceOf[DelayedItem[T]]
    val myEnd = createdMs + delayMs //延迟到最后的时间
    val yourEnd = delayed.createdMs + delayed.delayMs

    if(myEnd < yourEnd) -1
    else if(myEnd > yourEnd) 1
    else 0
  }
  
}
