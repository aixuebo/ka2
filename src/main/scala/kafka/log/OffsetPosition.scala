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

package kafka.log

/**
 * The mapping between a logical log offset and the physical position
 * in some log file of the beginning of the message set entry with the
 * given offset.
 * 参数
 * offset:表示该message的序号
 * position:表示在log中该offset位置的message第一个字节的在该logSegment中偏移量,从该位置可以读取该message信息,position位置的第一个字节信息是message12个字节的头文件,分别表示该message的序号和字节大小
 * 
 * 该对象表示log日志中一个message信息
 */
case class OffsetPosition(val offset: Long, val position: Int)