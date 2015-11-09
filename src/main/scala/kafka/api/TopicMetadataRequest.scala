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

import java.nio.ByteBuffer
import kafka.api.ApiUtils._
import collection.mutable.ListBuffer
import kafka.network.{BoundedByteBufferSend, RequestChannel}
import kafka.common.ErrorMapping
import kafka.network.RequestChannel.Response
import kafka.utils.Logging

/**
 * 为topic抓取元数据的请求
 */
object TopicMetadataRequest extends Logging {
  val CurrentVersion = 0.shortValue//当前版本号
  val DefaultClientId = ""

  /**
   * TopicMetadataRequest has the following format -
   * number of topics (4 bytes) list of topics (2 bytes + topic.length per topic) detailedMetadata (2 bytes) timestamp (8 bytes) count (4 bytes)
   * 从ByteBuffer中反序列化TopicMetadataRequest对象
   */
  def readFrom(buffer: ByteBuffer): TopicMetadataRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val numTopics = readIntInRange(buffer, "number of topics", (0, Int.MaxValue))
    val topics = new ListBuffer[String]()
    for(i <- 0 until numTopics)
      topics += readShortString(buffer)
    new TopicMetadataRequest(versionId, correlationId, clientId, topics.toList)
  }
}

case class TopicMetadataRequest(val versionId: Short,//当前请求的版本号,根据kafka源码来编译,一版不会改动
                                val correlationId: Int,
                                val clientId: String,
                                val topics: Seq[String])//等待获取元数据的的topic集合
 extends RequestOrResponse(Some(RequestKeys.MetadataKey)){

  def this(topics: Seq[String], correlationId: Int) =
    this(TopicMetadataRequest.CurrentVersion, correlationId, TopicMetadataRequest.DefaultClientId, topics)

    //将信息写入到ByteBuffer中
  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(topics.size)
    topics.foreach(topic => writeShortString(buffer, topic))
  }

  def sizeInBytes(): Int = {
    2 +  /* version id */
    4 + /* correlation id */
    shortStringLength(clientId)  + /* client id */
    4 + /* number of topics */
    topics.foldLeft(0)(_ + shortStringLength(_)) /* topics */
  }

  override def toString(): String = {
    describe(true)
  }

  //当请求有异常的时候调用该方法
  override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    //为每一个topic设置返回的TopicMetadata对象,错误码通过异常class映射
    val topicMetadata = topics.map {
      topic => TopicMetadata(topic, Nil, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    val errorResponse = TopicMetadataResponse(Seq(), topicMetadata, correlationId)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val topicMetadataRequest = new StringBuilder
    topicMetadataRequest.append("Name: " + this.getClass.getSimpleName)
    topicMetadataRequest.append("; Version: " + versionId)
    topicMetadataRequest.append("; CorrelationId: " + correlationId)
    topicMetadataRequest.append("; ClientId: " + clientId)
    if(details)
      topicMetadataRequest.append("; Topics: " + topics.mkString(","))
    topicMetadataRequest.toString()
  }
}