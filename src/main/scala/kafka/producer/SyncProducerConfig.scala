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

package kafka.producer

import java.util.Properties
import kafka.utils.VerifiableProperties

/**
 * 该生产者向哪个host和port发送请求
 */
class SyncProducerConfig private (val props: VerifiableProperties) extends SyncProducerConfigShared {
  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    // no need to verify the property since SyncProducerConfig is supposed to be used internally
  }

  /** the broker to which the producer sends events */
  val host = props.getString("host")

  /** the port on which the broker is running */
  val port = props.getInt("port")
}

trait SyncProducerConfigShared {
  val props: VerifiableProperties
  
  val sendBufferBytes = props.getInt("send.buffer.bytes", 100*1024)

  /* the client application sending the producer requests */
  val clientId = props.getString("client.id", SyncProducerConfig.DefaultClientId)

  /*
   * The number of acknowledgments the producer requires the leader to have received before considering a request complete.
   * This controls the durability of the messages sent by the producer.
   *
   * request.required.acks = 0 - means the producer will not wait for any acknowledgement from the leader.
   * request.required.acks = 1 - means the leader will write the message to its local log and immediately acknowledge
   * request.required.acks = -1 - means the leader will wait for acknowledgement from all in-sync replicas before acknowledging the write
   * 确认字符,在[-1,1]之间,因为只有-1 1 0三个
   * 
用来控制一个produce请求怎样才能算完成，准确的说，是有多少broker必须已经提交数据到log文件，并向leader发送ack，可以设置如下的值：
0，意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。这个选项提供了最低的延迟，但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
1，意味着在leader replica已经接收到数据后，producer会得到一个ack。这个选项提供了更好的持久性，因为在server确认请求成功处理后，client才会返回。如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
-1，意味着在所有的ISR都接收到数据后，producer才得到一个ack。这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失。
   */
  val requestRequiredAcks = props.getShortInRange("request.required.acks", SyncProducerConfig.DefaultRequiredAcks,(-1,1))

  /*
   * The ack timeout of the producer requests. Value must be non-negative and non-zero
   */
  val requestTimeoutMs = props.getIntInRange("request.timeout.ms", SyncProducerConfig.DefaultAckTimeoutMs,
                                             (1, Integer.MAX_VALUE))
}

object SyncProducerConfig {
  val DefaultClientId = ""
  val DefaultRequiredAcks : Short = 0
  val DefaultAckTimeoutMs = 10000
}
