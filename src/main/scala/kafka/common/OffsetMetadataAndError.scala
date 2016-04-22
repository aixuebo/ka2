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

//记录偏移量和一个String的描述信息以及错误编码
case class OffsetAndMetadata(offset: Long,
                             metadata: String = OffsetAndMetadata.NoMetadata,
                             var timestamp: Long = -1L) {
  override def toString = "OffsetAndMetadata[%d,%s%s]"
                          .format(offset,
                                  if (metadata != null && metadata.length > 0) metadata else "NO_METADATA",
                                  if (timestamp == -1) "" else "," + timestamp.toString)
}

object OffsetAndMetadata {
  val InvalidOffset: Long = -1L
  val NoMetadata: String = ""
  val InvalidTime: Long = -1L
}

/**
 * 记录offset偏移量以及描述信息  以及异常原因
 */
case class OffsetMetadataAndError(offset: Long,
                                  metadata: String = OffsetAndMetadata.NoMetadata,
                                  error: Short = ErrorMapping.NoError) {

  def this(offsetMetadata: OffsetAndMetadata, error: Short) =
    this(offsetMetadata.offset, offsetMetadata.metadata, error)

  def this(error: Short) =
    this(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, error)

  def asTuple = (offset, metadata, error)

  override def toString = "OffsetMetadataAndError[%d,%s,%d]".format(offset, metadata, error)
}

object OffsetMetadataAndError {
  //没有topic-partition对应的offset数据
  val NoOffset = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.NoError)
  //说明该partition正在加载中,还没有加载完成
  val OffsetsLoading = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.OffsetsLoadInProgressCode)
  //说明本地磁盘上没有该partition对应的文件
  val NotOffsetManagerForGroup = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.NotCoordinatorForConsumerCode)
  //不存在该topic-partition
  val UnknownTopicOrPartition = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.UnknownTopicOrPartitionCode)
}

