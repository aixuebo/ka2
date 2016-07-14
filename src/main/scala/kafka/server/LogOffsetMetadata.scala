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

package kafka.server

import org.apache.kafka.common.KafkaException

object LogOffsetMetadata {
  val UnknownOffsetMetadata = new LogOffsetMetadata(-1, 0, 0)
  val UnknownSegBaseOffset = -1L
  val UnknownFilePosition = -1

  class OffsetOrdering extends Ordering[LogOffsetMetadata] {
    override def compare(x: LogOffsetMetadata , y: LogOffsetMetadata ): Int = {
      return x.offsetDiff(y).toInt
    }
  }

}

/*
 * A log offset structure, including:
 *  1. the message offset 当前下一个message的序号,该序号就是全局的序号
 *  2. the base message offset of the located segment 当前最后一个日志中第一个元素的序号
 *  3. the physical position on the located segment 在当前的日志中字节偏移量位置
 */
case class LogOffsetMetadata(messageOffset: Long,
                             segmentBaseOffset: Long = LogOffsetMetadata.UnknownSegBaseOffset,
                             relativePositionInSegment: Int = LogOffsetMetadata.UnknownFilePosition) {

  // check if this offset is already on an older segment compared with the given offset
  def offsetOnOlderSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly())
      throw new KafkaException("%s cannot compare its segment info with %s since it only has message offset info".format(this, that))

    this.segmentBaseOffset < that.segmentBaseOffset
  }

  // check if this offset is on the same segment with the given offset
  //true表示比较的两个是同一个segment文件,因为他们的start开始序号是相同的
  def offsetOnSameSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly())
      throw new KafkaException("%s cannot compare its segment info with %s since it only has message offset info".format(this, that))

    this.segmentBaseOffset == that.segmentBaseOffset
  }

  // check if this offset is before the given offset,true:当前的 比 参数小
  def precedes(that: LogOffsetMetadata): Boolean = this.messageOffset < that.messageOffset

  // compute the number of messages between this offset to the given offset比较当前的和参数的之差,相差多少个序号
  def offsetDiff(that: LogOffsetMetadata): Long = {
    this.messageOffset - that.messageOffset
  }

  // compute the number of bytes between this offset to the given offset
  // if they are on the same segment and this offset precedes the given offset
  //当两个文件是通过一个segment文件的时候,比较第三个参数之差,即相差多少个字节,只能是两个文件相同的时候才能调用该方法
  def positionDiff(that: LogOffsetMetadata): Int = {
    if(!offsetOnSameSegment(that))
      throw new KafkaException("%s cannot compare its segment position with %s since they are not on the same segment".format(this, that))
    if(messageOffsetOnly())
      throw new KafkaException("%s cannot compare its segment position with %s since it only has message offset info".format(this, that))

    this.relativePositionInSegment - that.relativePositionInSegment
  }

  // decide if the offset metadata only contains message offset info
  //true表示仅仅有第一个参数,没有第二个和第三个参数
  def messageOffsetOnly(): Boolean = {
    segmentBaseOffset == LogOffsetMetadata.UnknownSegBaseOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition
  }

  override def toString = messageOffset.toString + " [" + segmentBaseOffset + " : " + relativePositionInSegment + "]"

}
