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

package kafka.message

import kafka.serializer.Decoder
import kafka.utils.Utils

/**
 * 代表一个消息的全部元数据信息
 * 包含topic、partition、消息信息的字节原始信息、该消息所在的偏移量、该消息的key和value的反序列化对象,即如果把字节数组转换成对象的class
 */
case class MessageAndMetadata[K, V](topic: String, partition: Int,
                                    private val rawMessage: Message, offset: Long,
                                    keyDecoder: Decoder[K], valueDecoder: Decoder[V]) {

  /**
   * Return the decoded message key and payload
   * 将消息中的key通过key反序列化class转换成k对象
   */
  def key(): K = if(rawMessage.key == null) null.asInstanceOf[K] else keyDecoder.fromBytes(Utils.readBytes(rawMessage.key))

  /**
   * 将消息中的value通过value反序列化class转换成v对象
   */
  def message(): V = if(rawMessage.isNull) null.asInstanceOf[V] else valueDecoder.fromBytes(Utils.readBytes(rawMessage.payload))
}

