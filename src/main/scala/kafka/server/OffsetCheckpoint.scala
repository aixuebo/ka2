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

import scala.collection._
import kafka.utils.Logging
import kafka.common._
import java.io._

/**
 * This class saves out a map of topic/partition=>offsets to a file
 * 该类的目的:将每一个topic-partition 对应的offset偏移量写入到file文件中
 * 
 * 如果该类下没有tmp文件,则以file稳准
 * 如果该类下有tmp,又有file,则以file为准,因为tmp可能正在生成中,程序出问题了
 * 如果该类下有tmp,没有file,则说明tmp是最新版,因为有tmp后,就将file删除掉了,所以只有tmp,没有file
 */
class OffsetCheckpoint(val file: File) extends Logging {
  private val lock = new Object()
  new File(file + ".tmp").delete() // try to delete any existing temp files for cleanliness
  file.createNewFile() // in case the file doesn't exist

  def write(offsets: Map[TopicAndPartition, Long]) {
    lock synchronized {
      
      //创建.tmp临时文件,并且向临时文件写入内容
      // write to temp file and then swap with the existing file
      val temp = new File(file.getAbsolutePath + ".tmp")

      val fileOutputStream = new FileOutputStream(temp)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {
        // write the current version,写入版本号
        writer.write(0.toString)
        writer.newLine()
      
        // write the number of entries 写入多少组topic-partition
        writer.write(offsets.size.toString)
        writer.newLine()

        //循环每一组topic-partition,写入topic partition offset偏移量
        // write the entries
        offsets.foreach { case (topicPart, offset) =>
          writer.write("%s %d %d".format(topicPart.topic, topicPart.partition, offset))
          writer.newLine()
        }
      
        // flush the buffer and then fsync the underlying file
        writer.flush()
        fileOutputStream.getFD().sync()
      } finally {
        writer.close()
      }
      
      //写入完成后将临时文件改名成正确的文件
      // swap new offset checkpoint file with previous one
      if(!temp.renameTo(file)) {
        // renameTo() fails on Windows if the destination file exists.在windows上文件不允许重名,因此删除原始file文件,因为该文件已经没用了,现在应该以tmp为准
        file.delete()
        if(!temp.renameTo(file))//再次切换名称,如果还有异常则抛出
          throw new IOException("File rename from %s to %s failed.".format(temp.getAbsolutePath, file.getAbsolutePath))
      }
    }
  }

  //从file文件中还原topic-partition对应的offset
  def read(): Map[TopicAndPartition, Long] = {
    lock synchronized {
      val reader = new BufferedReader(new FileReader(file))
      try {
        var line = reader.readLine()
        if(line == null)
          return Map.empty
        
        //读取版本号
        val version = line.toInt
        version match {
          case 0 =>//校验版本号
            line = reader.readLine()
            if(line == null)
              return Map.empty
              //获取有多少组topic-partition
            val expectedSize = line.toInt
            var offsets = Map[TopicAndPartition, Long]()
            line = reader.readLine()
            
            while(line != null) {//解析每一行数据都对应一组topic-partition offset
              val pieces = line.split("\\s+")
              if(pieces.length != 3)
                throw new IOException("Malformed line in offset checkpoint file: '%s'.".format(line))
              
              val topic = pieces(0)
              val partition = pieces(1).toInt
              val offset = pieces(2).toLong
              offsets += (TopicAndPartition(topic, partition) -> offset)
              line = reader.readLine()
            }
            if(offsets.size != expectedSize)
              throw new IOException("Expected %d entries but found only %d".format(expectedSize, offsets.size))
            offsets
          case _ => //校验版本号失败
            throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
        }
      } finally {
        reader.close()
      }
    }
  }
  
}