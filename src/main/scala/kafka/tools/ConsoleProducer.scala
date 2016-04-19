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

package kafka.tools

import kafka.common._
import kafka.message._
import kafka.serializer._
import kafka.utils.{ToolsUtils, CommandLineUtils}
import kafka.producer.{NewShinyProducer,OldProducer,KeyedMessage}

import java.util.Properties
import java.io._

import joptsimple._

//从控制台产生的数据发送给broker,即控制台作为生产者
object ConsoleProducer {

  def main(args: Array[String]) {

    val config = new ProducerConfig(args)//全局生产者配置参数
    val reader = Class.forName(config.readerClass).newInstance().asInstanceOf[MessageReader] //生产者如何获取数据信息
    val props = new Properties
    props.put("topic", config.topic)
    props.putAll(config.cmdLineProps)
    reader.init(System.in, props)

    try {
        val producer =
          if(config.useNewProducer) {//新的生产者
            import org.apache.kafka.clients.producer.ProducerConfig

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionCodec)
            props.put(ProducerConfig.SEND_BUFFER_CONFIG, config.socketBuffer.toString)
            props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs.toString)
            props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, config.metadataExpiryMs.toString)
            props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, config.metadataFetchTimeoutMs.toString)
            props.put(ProducerConfig.ACKS_CONFIG, config.requestRequiredAcks.toString)
            props.put(ProducerConfig.TIMEOUT_CONFIG, config.requestTimeoutMs.toString)
            props.put(ProducerConfig.RETRIES_CONFIG, config.messageSendMaxRetries.toString)
            props.put(ProducerConfig.LINGER_MS_CONFIG, config.sendTimeout.toString)
            if(config.queueEnqueueTimeoutMs != -1)
              props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false")
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.maxMemoryBytes.toString)
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.maxPartitionMemoryBytes.toString)
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "console-producer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

            new NewShinyProducer(props)
          } else {//老的生产者
            props.put("metadata.broker.list", config.brokerList)
            props.put("compression.codec", config.compressionCodec)
            props.put("producer.type", if(config.sync) "sync" else "async")
            props.put("batch.num.messages", config.batchSize.toString)
            props.put("message.send.max.retries", config.messageSendMaxRetries.toString)
            props.put("retry.backoff.ms", config.retryBackoffMs.toString)
            props.put("queue.buffering.max.ms", config.sendTimeout.toString)
            props.put("queue.buffering.max.messages", config.queueSize.toString)
            props.put("queue.enqueue.timeout.ms", config.queueEnqueueTimeoutMs.toString)
            props.put("request.required.acks", config.requestRequiredAcks.toString)
            props.put("request.timeout.ms", config.requestTimeoutMs.toString)
            props.put("key.serializer.class", config.keyEncoderClass)
            props.put("serializer.class", config.valueEncoderClass)
            props.put("send.buffer.bytes", config.socketBuffer.toString)
            props.put("topic.metadata.refresh.interval.ms", config.metadataExpiryMs.toString)
            props.put("client.id", "console-producer")

            new OldProducer(props)
          }

        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run() {
            producer.close()
          }
        })

        var message: KeyedMessage[Array[Byte], Array[Byte]] = null
        do {
          message = reader.readMessage()
          if(message != null)
            producer.send(message.topic, message.key, message.message)//生产者向topic发送key和value信息
        } while(message != null)
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
    System.exit(0)
  }

  class ProducerConfig(args: Array[String]) {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("broker-list")
      .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
    val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', or 'lz4'." +
                                                                  "If specified without value, then it defaults to 'gzip'")
                                    .withOptionalArg()
                                    .describedAs("compression-codec")
                                    .ofType(classOf[String])//数据上传到broker时候的压缩方式,'none', 'gzip', 'snappy', or 'lz4'
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)//批处理大小,异步操作时,一次发送多少个信息到broker
    val messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, and being unavailable transiently is just one of them. This property specifies the number of retires before the producer give up and drop this message.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3) //broker能够在很多理由下,接收信息失败,他们可能仅仅在一段期间内不能接收信息,因此该配置只是了尝试发送次数,超过了该次数,则该信息要被丢弃或者其他处理
    val retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100) //生产者每次刷新关联的topic的元数据时,含义现在不明确,需要看代码
    val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
      " a message will queue awaiting suffient batch size. The value is given in ms.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])//在异步队列中,满足该时间后,队列的信息也要被发生到生产者
      .defaultsTo(1000)
    val queueSizeOpt = parser.accepts("queue-size", "If set and the producer is running in asynchronous mode, this gives the maximum amount of " +
      " messages will queue awaiting suffient batch size.")
      .withRequiredArg
      .describedAs("queue_size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10000) //如果生产者是异步的,则给信息的队列设置长度
    val queueEnqueueTimeoutMsOpt = parser.accepts("queue-enqueuetimeout-ms", "Timeout for event enqueue")
      .withRequiredArg
      .describedAs("queue enqueuetimeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(Int.MaxValue) //在队列中入队的超时时间
    val requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required acks of the producer requests")
      .withRequiredArg
      .describedAs("request required acks")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)//判断请求是否要有确认操作
    val requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero")
      .withRequiredArg
      .describedAs("request timeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1500) //该值必须是大于0的,表示ack确认的超时时间
    val metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
      "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes.")
      .withRequiredArg
      .describedAs("metadata expiration interval")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(5*60*1000L) //元数据的过期时间,我们强制一个刷新元数据的时间,甚至我们没有任何更改的情况下也会去周期的刷新元数据信息
    val metadataFetchTimeoutMsOpt = parser.accepts("metadata-fetch-timeout-ms",
      "The amount of time to block waiting to fetch metadata about a topic the first time a record is sent to that topic.")
      .withRequiredArg
      .describedAs("metadata fetch timeout")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(60*1000L) //阻塞等候去抓去关于topic的元数据的超市时间,第一个记录要发送给某个topic时候使用,去获取该topic的元数据信息
    val maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
      "The total memory used by the producer to buffer records waiting to be sent to the server.")
      .withRequiredArg
      .describedAs("total memory in bytes")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(32 * 1024 * 1024L) //总的最大字节数,等待发送到server前最大的内存数
    val maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
      "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
        "will attempt to optimistically group them together until this size is reached.")
      .withRequiredArg
      .describedAs("memory in bytes per partition")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(16 * 1024L) //为一个partition分配的最大内存缓冲区的字节数
    val valueEncoderOpt = parser.accepts("value-serializer", "The class name of the message encoder implementation to use for serializing values.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)//如何对value进行编码的类
    val keyEncoderOpt = parser.accepts("key-serializer", "The class name of the message encoder implementation to use for serializing keys.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)//如何对key进行编码的类
    val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
      "By default each line is read as a separate message.")
      .withRequiredArg
      .describedAs("reader_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[LineMessageReader].getName) //如何读取一行信息的class类名,默认是LineMessageReader
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024*100)//设置tpc的RECV大小,即tcp的缓冲大小
    val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the message reader. " +
      "This allows custom configuration for a user-defined message reader.")
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])//一个机制去让用户传递自定义的属性信息,格式是key=value
    val useNewProducerOpt = parser.accepts("new-producer", "Use the new producer implementation.")//使用新的生产者还是老的生产者

    //解析参数
    val options = parser.parse(args : _*)
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Read data from standard input and publish it to Kafka.")
    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, brokerListOpt)

    import scala.collection.JavaConversions._
    val useNewProducer = options.has(useNewProducerOpt)
    val topic = options.valueOf(topicOpt)
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser,brokerList)
    val sync = options.has(syncOpt)
    val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
    val compressionCodec = if (options.has(compressionCodecOpt))
                             if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
                               DefaultCompressionCodec.name
                             else compressionCodecOptionValue
                           else NoCompressionCodec.name
    val batchSize = options.valueOf(batchSizeOpt)
    val sendTimeout = options.valueOf(sendTimeoutOpt)
    val queueSize = options.valueOf(queueSizeOpt)
    val queueEnqueueTimeoutMs = options.valueOf(queueEnqueueTimeoutMsOpt)
    val requestRequiredAcks = options.valueOf(requestRequiredAcksOpt)
    val requestTimeoutMs = options.valueOf(requestTimeoutMsOpt)
    val messageSendMaxRetries = options.valueOf(messageSendMaxRetriesOpt)
    val retryBackoffMs = options.valueOf(retryBackoffMsOpt)
    val keyEncoderClass = options.valueOf(keyEncoderOpt)
    val valueEncoderClass = options.valueOf(valueEncoderOpt)
    val readerClass = options.valueOf(messageReaderOpt)
    val socketBuffer = options.valueOf(socketBufferSizeOpt)
    val cmdLineProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt)) //获取用户自定义的配置信息,即用户自定义key=value格式的信息
    /* new producer related configs */
    val maxMemoryBytes = options.valueOf(maxMemoryBytesOpt)
    val maxPartitionMemoryBytes = options.valueOf(maxPartitionMemoryBytesOpt)
    val metadataExpiryMs = options.valueOf(metadataExpiryMsOpt)
    val metadataFetchTimeoutMs = options.valueOf(metadataFetchTimeoutMsOpt)
  }

  //信息读取抽象类
  trait MessageReader {
    def init(inputStream: InputStream, props: Properties) {} //初始化信息
    def readMessage(): KeyedMessage[Array[Byte], Array[Byte]]//读取一行信息,key和value的类型都是Array[Byte]
    def close() {}
  }

  class LineMessageReader extends MessageReader {
    var topic: String = null //该文件属于什么topic
    var reader: BufferedReader = null //读取文件的流
    var parseKey = false //true表示有key要被解析
    var keySeparator = "\t" //表示key的拆分符号
    var ignoreError = false //true表示忽略异常
    var lineNumber = 0 //处理了多少行数据了

    override def init(inputStream: InputStream, props: Properties) {
      topic = props.getProperty("topic")
      if(props.containsKey("parse.key"))
        parseKey = props.getProperty("parse.key").trim.toLowerCase.equals("true")
      if(props.containsKey("key.separator"))
        keySeparator = props.getProperty("key.separator")
      if(props.containsKey("ignore.error"))
        ignoreError = props.getProperty("ignore.error").trim.toLowerCase.equals("true")
      reader = new BufferedReader(new InputStreamReader(inputStream))
    }

    override def readMessage() = {
      lineNumber += 1
      (reader.readLine(), parseKey) match {
        case (null, _) => null
        case (line, true) => //说明有key存在
          line.indexOf(keySeparator) match {//查找到key的分隔位置
            case -1 =>
              if(ignoreError)//说明没有key分隔符,并且忽略异常,则当没有key处理,所有信息都是value
                new KeyedMessage[Array[Byte], Array[Byte]](topic, line.getBytes())
              else
                throw new KafkaException("No key found on line " + lineNumber + ": " + line) //没有key,则抛异常
            case n =>
              new KeyedMessage[Array[Byte], Array[Byte]](topic,
                             line.substring(0, n).getBytes,//获取key
                             (if(n + keySeparator.size > line.size) "" else line.substring(n + keySeparator.size)).getBytes())//获取value,如果字符长度不够了,则说明value为""
          }
        case (line, false) => //说明没有key存在
          new KeyedMessage[Array[Byte], Array[Byte]](topic, line.getBytes()) //仅将一行信息都组装成value即可
      }
    }
  }
}
