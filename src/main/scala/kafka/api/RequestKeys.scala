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

import kafka.common.KafkaException
import java.nio.ByteBuffer

//请求的分类,真正的处理逻辑参见KafkaApis类实现
object RequestKeys {
  val ProduceKey: Short = 0//生产者请求
  val FetchKey: Short = 1//抓取某些个topic的某些partition,从offset开始,抓取fetchSize个字节数据,同时也用于partition数据从follow节点同步leader节点数据,参见ReplicaFetcherThread、AbstractFetcherThread
  val OffsetsKey: Short = 2//获取topic-partition当前的offset偏移量
  val MetadataKey: Short = 3//获取topic元数据信息请求,参见TopicMetadataRequest,返回值包含TopicMetadata
  val LeaderAndIsrKey: Short = 4//更新每一个topic-partition对应的leader和同步节点集合,参见LeaderAndIsrRequest
  val StopReplicaKey: Short = 5
  val UpdateMetadataKey: Short = 6//更新现在活着的brokerId,以及topic-partition-PartitionStateInfo映射关系,参见UpdateMetadataRequest和MetadataCache
  val ControlledShutdownKey: Short = 7 //服务器要shutdown,因此向controller发送信息
  val OffsetCommitKey: Short = 8 //更新topic-partition的offset请求,版本号0的时候走一套老逻辑,版本号>0,则就当生产者请求处理,向特定topic存储该信息
  val OffsetFetchKey: Short = 9//抓去每一个topic-partition对应的offset最新信息
  val ConsumerMetadataKey: Short = 10//获取制定group所在的offset的topic所在partition的leader节点
  val JoinGroupKey: Short = 11
  val HeartbeatKey: Short = 12

  //Map[Short, (String, (ByteBuffer) => RequestOrResponse)] 表示map的key是short,value是元组,由name和RequestOrResponse组成,其中RequestOrResponse是由一个输入ByteBuffer参数的方法生成的
  val keyToNameAndDeserializerMap: Map[Short, (String, (ByteBuffer) => RequestOrResponse)]=
    Map(ProduceKey -> ("Produce", ProducerRequest.readFrom),
        FetchKey -> ("Fetch", FetchRequest.readFrom),
        OffsetsKey -> ("Offsets", OffsetRequest.readFrom),
        MetadataKey -> ("Metadata", TopicMetadataRequest.readFrom),
        LeaderAndIsrKey -> ("LeaderAndIsr", LeaderAndIsrRequest.readFrom),
        StopReplicaKey -> ("StopReplica", StopReplicaRequest.readFrom),
        UpdateMetadataKey -> ("UpdateMetadata", UpdateMetadataRequest.readFrom),
        ControlledShutdownKey -> ("ControlledShutdown", ControlledShutdownRequest.readFrom),
        OffsetCommitKey -> ("OffsetCommit", OffsetCommitRequest.readFrom),
        OffsetFetchKey -> ("OffsetFetch", OffsetFetchRequest.readFrom),
        ConsumerMetadataKey -> ("ConsumerMetadata", ConsumerMetadataRequest.readFrom),
        JoinGroupKey -> ("JoinGroup", JoinGroupRequestAndHeader.readFrom),
        HeartbeatKey -> ("Heartbeat", HeartbeatRequestAndHeader.readFrom)
    )

    //返回编号对应的String类型name
  def nameForKey(key: Short): String = {
    keyToNameAndDeserializerMap.get(key) match {
      case Some(nameAndSerializer) => nameAndSerializer._1
      case None => throw new KafkaException("Wrong request type %d".format(key))
    }
  }

  //返回编号对应的ByteBuffer类型RequestOrResponse
  /**
   * deserializerForKey(key: Short): (ByteBuffer) => RequestOrResponse = 表示deserializerForKey(key: Short)的返回值需要一个参数ByteBuffer,然后生成RequestOrResponse对象
   */
  def deserializerForKey(key: Short): (ByteBuffer) => RequestOrResponse = {
    keyToNameAndDeserializerMap.get(key) match {
      case Some(nameAndSerializer) => nameAndSerializer._2
      case None => throw new KafkaException("Wrong request type %d".format(key))
    }
  }
}
