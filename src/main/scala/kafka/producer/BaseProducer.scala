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

// A base producer used whenever we need to have options for both old and new producers;
// this class will be removed once we fully rolled out 0.9
trait BaseProducer {
  //将key-value发送给某个topic,具体的partition是不需要生产者操心的
  def send(topic: String, key: Array[Byte], value: Array[Byte])
  def close()
}

class NewShinyProducer(producerProps: Properties) extends BaseProducer {
  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
  import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback

  // decide whether to send synchronously based on producer properties 生产者是同步还是异步
  val sync = producerProps.getProperty("producer.type", "async").equals("sync")

  //创建一个生产者,key-value都是字节数组的生产者
  val producer = new KafkaProducer[Array[Byte],Array[Byte]](producerProps)

  override def send(topic: String, key: Array[Byte], value: Array[Byte]) {
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic, key, value)
    if(sync) {//同步方式发送
      this.producer.send(record).get()
    } else {//异步方式发送
      this.producer.send(record,
        new ErrorLoggingCallback(topic, key, value, false))
    }
  }

  override def close() {
    this.producer.close()
  }
}

class OldProducer(producerProps: Properties) extends BaseProducer {
  import kafka.producer.{KeyedMessage, ProducerConfig}

  // default to byte array partitioner 默认是使用ByteArrayPartitioner进行分区
  if (producerProps.getProperty("partitioner.class") == null)
    producerProps.setProperty("partitioner.class", classOf[kafka.producer.ByteArrayPartitioner].getName)
    
    //创建一个生产者,key-value都是字节数组的生产者
  val producer = new kafka.producer.Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerProps))

  override def send(topic: String, key: Array[Byte], value: Array[Byte]) {
    this.producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, key, value))
  }

  override def close() {
    this.producer.close()
  }
}

