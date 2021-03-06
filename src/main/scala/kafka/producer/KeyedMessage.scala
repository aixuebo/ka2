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

/**
 * A topic, key, and value.
 * If a partition key is provided it will override the key for the purpose of partitioning but will not be stored.
 * 
 * 包含一组消息,包含key-value、topic以及partKey
 * 因为生产者是不关注发送给哪个partition的,至于发送到哪个partiton上取决于key和partKey
 */
case class KeyedMessage[K, V](val topic: String, val key: K, val partKey: Any, val message: V) {
  if(topic == null)
    throw new IllegalArgumentException("Topic cannot be null.")
  
  def this(topic: String, message: V) = this(topic, null.asInstanceOf[K], null, message)
  
  def this(topic: String, key: K, message: V) = this(topic, key, key, message)
  
  /**
   * 如果partKey存在,则返回该partKey进行partition分组
   * 如果partKey不存在,但是key存在,则使用key进行partition分组
   * 否则返回null
   */
  def partitionKey = {
    if(partKey != null)
      partKey
    else if(hasKey)
      key
    else
      null  
  }
  
  //key是否存在
  def hasKey = key != null
}