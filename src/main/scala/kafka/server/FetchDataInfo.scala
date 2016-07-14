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

import kafka.message.MessageSet

//follower节点去leader节点抓取数据的返回值,参见Log对象的read方法
/**
 * @param fetchOffset 表示要抓去的第一个序号、所在segment文件中的第一个序号、当前序号在该segment文件中的偏移量
 * @param messageSet 表示要抓去的message的全部内容都在这些字节数组里面存放着呢
 */
case class FetchDataInfo(fetchOffset: LogOffsetMetadata, messageSet: MessageSet)
