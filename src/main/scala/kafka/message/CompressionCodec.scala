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

//压缩方式的定义
object CompressionCodec {
  
  //通过id返回压缩方式
  def getCompressionCodec(codec: Int): CompressionCodec = {
    codec match {
      case NoCompressionCodec.codec => NoCompressionCodec
      case GZIPCompressionCodec.codec => GZIPCompressionCodec
      case SnappyCompressionCodec.codec => SnappyCompressionCodec
      case LZ4CompressionCodec.codec => LZ4CompressionCodec
      case _ => throw new kafka.common.UnknownCodecException("%d is an unknown compression codec".format(codec))
    }
  }

  //通过name返回压缩方式
  def getCompressionCodec(name: String): CompressionCodec = {
    name.toLowerCase match {
      case NoCompressionCodec.name => NoCompressionCodec
      case GZIPCompressionCodec.name => GZIPCompressionCodec
      case SnappyCompressionCodec.name => SnappyCompressionCodec
      case LZ4CompressionCodec.name => LZ4CompressionCodec
      case _ => throw new kafka.common.UnknownCodecException("%s is an unknown compression codec".format(name))
    }
  }
}

//为压缩方式,声明id编码和name名称
sealed trait CompressionCodec { def codec: Int; def name: String }

case object DefaultCompressionCodec extends CompressionCodec {
  val codec = GZIPCompressionCodec.codec
  val name = GZIPCompressionCodec.name
}

case object GZIPCompressionCodec extends CompressionCodec {
  val codec = 1
  val name = "gzip"
}

case object SnappyCompressionCodec extends CompressionCodec {
  val codec = 2
  val name = "snappy"
}

case object LZ4CompressionCodec extends CompressionCodec {
  val codec = 3
  val name = "lz4"
}

case object NoCompressionCodec extends CompressionCodec {
  val codec = 0
  val name = "none"
}
