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

package kafka.common

import kafka.message.InvalidMessageException
import java.nio.ByteBuffer
import scala.Predef._

/**
 * A bi-directional mapping between error codes and exceptions
 * 异常的映射
 */
object ErrorMapping {
  val EmptyByteBuffer = ByteBuffer.allocate(0)

  val UnknownCode : Short = -1
  val NoError : Short = 0//没有异常
  val OffsetOutOfRangeCode : Short = 1
  val InvalidMessageCode : Short = 2
  val UnknownTopicOrPartitionCode : Short = 3 //没有topic-partition信息
  val InvalidFetchSizeCode  : Short = 4
  val LeaderNotAvailableCode : Short = 5 //leader不可用
  val NotLeaderForPartitionCode : Short = 6 //partition没有leader
  val RequestTimedOutCode: Short = 7
  val BrokerNotAvailableCode: Short = 8
  val ReplicaNotAvailableCode: Short = 9
  val MessageSizeTooLargeCode: Short = 10
  val StaleControllerEpochCode: Short = 11//controller的epoche版本号异常,即controller的枚举次数不对
  val OffsetMetadataTooLargeCode: Short = 12 //offset的描述信息过长
  val StaleLeaderEpochCode: Short = 13 //说明本地的partition的leader枚举次数比请求的还要大,因此说明该topic-partition有问题
  val OffsetsLoadInProgressCode: Short = 14//偏移量信息正在被加载,参见OffsetManager的getOffsets方法
  val ConsumerCoordinatorNotAvailableCode: Short = 15
  val NotCoordinatorForConsumerCode: Short = 16
  val InvalidTopicCode : Short = 17
  val MessageSetSizeTooLargeCode: Short = 18
  val NotEnoughReplicasCode : Short = 19
  val NotEnoughReplicasAfterAppendCode: Short = 20 //说明没有足够多的partition备份节点去备份数据

  private val exceptionToCode =
    Map[Class[Throwable], Short](
      classOf[OffsetOutOfRangeException].asInstanceOf[Class[Throwable]] -> OffsetOutOfRangeCode,
      classOf[InvalidMessageException].asInstanceOf[Class[Throwable]] -> InvalidMessageCode,
      classOf[UnknownTopicOrPartitionException].asInstanceOf[Class[Throwable]] -> UnknownTopicOrPartitionCode,
      classOf[InvalidMessageSizeException].asInstanceOf[Class[Throwable]] -> InvalidFetchSizeCode,
      classOf[NotLeaderForPartitionException].asInstanceOf[Class[Throwable]] -> NotLeaderForPartitionCode,
      classOf[LeaderNotAvailableException].asInstanceOf[Class[Throwable]] -> LeaderNotAvailableCode,
      classOf[RequestTimedOutException].asInstanceOf[Class[Throwable]] -> RequestTimedOutCode,
      classOf[BrokerNotAvailableException].asInstanceOf[Class[Throwable]] -> BrokerNotAvailableCode,
      classOf[ReplicaNotAvailableException].asInstanceOf[Class[Throwable]] -> ReplicaNotAvailableCode,
      classOf[MessageSizeTooLargeException].asInstanceOf[Class[Throwable]] -> MessageSizeTooLargeCode,
      classOf[ControllerMovedException].asInstanceOf[Class[Throwable]] -> StaleControllerEpochCode,
      classOf[OffsetMetadataTooLargeException].asInstanceOf[Class[Throwable]] -> OffsetMetadataTooLargeCode,
      classOf[OffsetsLoadInProgressException].asInstanceOf[Class[Throwable]] -> OffsetsLoadInProgressCode,
      classOf[ConsumerCoordinatorNotAvailableException].asInstanceOf[Class[Throwable]] -> ConsumerCoordinatorNotAvailableCode,
      classOf[NotCoordinatorForConsumerException].asInstanceOf[Class[Throwable]] -> NotCoordinatorForConsumerCode,
      classOf[InvalidTopicException].asInstanceOf[Class[Throwable]] -> InvalidTopicCode,
      classOf[MessageSetSizeTooLargeException].asInstanceOf[Class[Throwable]] -> MessageSetSizeTooLargeCode,
      classOf[NotEnoughReplicasException].asInstanceOf[Class[Throwable]] -> NotEnoughReplicasCode,
      classOf[NotEnoughReplicasAfterAppendException].asInstanceOf[Class[Throwable]] -> NotEnoughReplicasAfterAppendCode
    ).withDefaultValue(UnknownCode)

  /* invert the mapping */
  private val codeToException =
    (Map[Short, Class[Throwable]]() ++ exceptionToCode.iterator.map(p => (p._2, p._1))).withDefaultValue(classOf[UnknownException])

  def codeFor(exception: Class[Throwable]): Short = exceptionToCode(exception)

  def maybeThrowException(code: Short) =
    if(code != 0)
      throw codeToException(code).newInstance()

  def exceptionFor(code: Short) : Throwable = codeToException(code).newInstance()
}
