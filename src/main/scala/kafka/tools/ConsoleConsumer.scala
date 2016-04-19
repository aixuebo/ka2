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

import scala.collection.JavaConversions._
import org.I0Itec.zkclient._
import joptsimple._
import java.util.Properties
import java.util.Random
import java.io.PrintStream
import kafka.message._
import kafka.serializer._
import kafka.utils._
import kafka.metrics.KafkaMetricsReporter
import kafka.consumer.{Blacklist,Whitelist,ConsumerConfig,Consumer}

/**
 * Consumer that dumps messages out to standard out.
 * 控制台作为消费者
 */
object ConsoleConsumer extends Logging {

  def main(args: Array[String]) {
    val parser = new OptionParser
    val topicIdOpt = parser.accepts("topic", "The topic id to consume on.")
            .withRequiredArg
            .describedAs("topic")
            .ofType(classOf[String]) //表示要消费哪个topic
    val whitelistOpt = parser.accepts("whitelist", "Whitelist of topics to include for consumption.")
            .withRequiredArg
            .describedAs("whitelist")
            .ofType(classOf[String])
    val blacklistOpt = parser.accepts("blacklist", "Blacklist of topics to exclude from consumption.")
            .withRequiredArg
            .describedAs("blacklist")
            .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
            "Multiple URLS can be given to allow fail-over.")
            .withRequiredArg
            .describedAs("urls")
            .ofType(classOf[String])//格式host:port,连接zookeeper的url

    val consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
            .withRequiredArg
            .describedAs("config file")
            .ofType(classOf[String]) //消费者配置文件路径
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
            .withRequiredArg
            .describedAs("class")
            .ofType(classOf[String])
            .defaultsTo(classOf[DefaultMessageFormatter].getName) //如何将kafka的信息格式化输出出来
    val messageFormatterArgOpt = parser.accepts("property")
            .withRequiredArg
            .describedAs("prop")
            .ofType(classOf[String]) //用户自定义属性信息
            
    //发现group消费组以前设置的offset元数据信息,请使用--delete-consumer-offsets命令去删除以前的offsets元数据信息
    //如果设置了,则消费组在启动的时候,要在zookeeper上被删除掉
    val deleteConsumerOffsetsOpt = parser.accepts("delete-consumer-offsets", "If specified, the consumer path in zookeeper is deleted when starting up");
    
    val resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
            "start with the earliest message present in the log rather than the latest message.")
            
    val maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
            .withRequiredArg
            .describedAs("num_messages")
            .ofType(classOf[java.lang.Integer]) //在退出消费者信息前,最多消费多少条记录,如果不设置,则会连续不断的全部消费所有数据
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
            "skip it instead of halt.")//表示当处理kafka中的key-value为一行数据时,如果出现异常,是跳过该一行信息,还是抛异常停止程序
            
    //如果设置了该参数,则csv格式的统计报告要被激活可用
    val csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled")
    //如果csvMetricsReporterEnabledOpt被设置了,则该命令也要被设置,用于将csv格式的统计信息存储在什么地方
    val metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
            "set, the csv metrics will be outputed here")
      .withRequiredArg
      .describedAs("metrics dictory")
      .ofType(classOf[java.lang.String])

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "The console consumer is a tool that reads data from Kafka and outputs it to standard output.")
      
    var groupIdPassed = true //false表示配置文件中没有设置groupId,true表示已经设置了
    val options: OptionSet = tryParse(parser, args)
    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt) //必须有zookeeper的url
    val topicOrFilterOpt = List(topicIdOpt, whitelistOpt, blacklistOpt).filter(options.has)
    if (topicOrFilterOpt.size != 1)
      CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/blacklist/topic is required.")//whitelist/blacklist/topic中必须只能存在一个
    val topicArg = options.valueOf(topicOrFilterOpt.head) //因为只有一个,所以获取head即可
    val filterSpec = if (options.has(blacklistOpt)) //判断是否有黑名单,如果有黑名单,则说明仅有的1个就是黑名单,否则都是白名单
      new Blacklist(topicArg)
    else
      new Whitelist(topicArg)

    val csvMetricsReporterEnabled = options.has(csvMetricsReporterEnabledOpt) //是否存储csv格式的报告信息
    if (csvMetricsReporterEnabled) {
      val csvReporterProps = new Properties() //配置信息
      csvReporterProps.put("kafka.metrics.polling.interval.secs", "5")
      csvReporterProps.put("kafka.metrics.reporters", "kafka.metrics.KafkaCSVMetricsReporter")
      if (options.has(metricsDirectoryOpt))
        csvReporterProps.put("kafka.csv.metrics.dir", options.valueOf(metricsDirectoryOpt))
      else
        csvReporterProps.put("kafka.csv.metrics.dir", "kafka_metrics")
      csvReporterProps.put("kafka.csv.metrics.reporter.enabled", "true")
      val verifiableProps = new VerifiableProperties(csvReporterProps)
      KafkaMetricsReporter.startReporters(verifiableProps)
    }

    val consumerProps = if (options.has(consumerConfigOpt)) //加载配置文件
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()//否则生成一个空的配置文件对象

    if(!consumerProps.containsKey("group.id")) {//没有设置消费的组ID
      consumerProps.put("group.id","console-consumer-" + new Random().nextInt(100000)) //随机生成一个组id
      groupIdPassed=false
    }
    consumerProps.put("auto.offset.reset", if(options.has(resetBeginningOpt)) "smallest" else "largest")
    consumerProps.put("zookeeper.connect", options.valueOf(zkConnectOpt)) //设置zookeeper连接url

    //该if表示没有设置delete-consumer-offsets命令,但是有from-beginning命令,同时该消费组下存在offsets路径
    if (!options.has(deleteConsumerOffsetsOpt) && options.has(resetBeginningOpt) &&
       checkZkPathExists(options.valueOf(zkConnectOpt),"/consumers/" + consumerProps.getProperty("group.id")+ "/offsets")) {
      System.err.println("Found previous offset information for this group "+consumerProps.getProperty("group.id")
        +". Please use --delete-consumer-offsets to delete previous offsets metadata")
        //发现group消费组以前设置的offset元数据信息,请使用--delete-consumer-offsets命令去删除以前的offsets元数据信息
      System.exit(1)//退出程序
    }

    if(options.has(deleteConsumerOffsetsOpt))//先删除该消费组的信息
      ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + consumerProps.getProperty("group.id"))

    val config = new ConsumerConfig(consumerProps) //消费者配置信息
    val skipMessageOnError = if (options.has(skipMessageOnErrorOpt)) true else false //处理kafka数据时,出现异常了,是否要跳过该行信息
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt)) //如何处理kafka的数据类
    val formatterArgs = CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt)) //用户自定义属性信息
    val maxMessages = if(options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1 //在退出消费者信息前,最多消费多少条记录,如果不设置,则会连续不断的全部消费所有数据
    val connector = Consumer.create(config) //创建消费者

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        connector.shutdown()
        // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
        if(!groupIdPassed) //因为没有设置组ID,而是我们随机生成的一个组ID,因此要删除组ID的信息
          ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + consumerProps.get("group.id"))
      }
    })

    var numMessages = 0L
    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter] //消费者消费key-value的class类
    formatter.init(formatterArgs) //使用用户自定义属性信息对该类进行初始化
    try {
      //获取topic集合、多少个流去获取、key和value的解析器,返回Set,Set元素是每一个流,因为当前需要1个流,因此get(0)即可返回流
      val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0)
      val iter = if(maxMessages >= 0)
        stream.slice(0, maxMessages)
      else
        stream

      for(messageAndTopic <- iter) {
        try {
          formatter.writeTo(messageAndTopic.key, messageAndTopic.message, System.out)//对每一个kafka里面的key-value进行输出到System.out中
          numMessages += 1 //处理了多少行数据
        } catch {
          case e: Throwable =>
            if (skipMessageOnError) //出异常时,是否要跳过该行信息
              error("Error processing message, skipping this message: ", e)
            else
              throw e
        }
        if(System.out.checkError()) {
          // This means no one is listening to our output stream any more, time to shutdown
          System.err.println("Unable to write to standard out, closing consumer.")
          System.err.println("Consumed %d messages".format(numMessages)) //消费了多少行数据
          formatter.close()
          connector.shutdown()
          System.exit(1)
        }
      }
    } catch {
      case e: Throwable => error("Error processing message, stopping consumer: ", e)
    }
    System.err.println("Consumed %d messages".format(numMessages))
    System.out.flush()
    formatter.close()
    connector.shutdown()
  }

  //使用cli工具解析参数
  def tryParse(parser: OptionParser, args: Array[String]) = {
    try {
      parser.parse(args : _*)
    } catch {
      case e: OptionException => {
        Utils.croak(e.getMessage)
        null
      }
    }
  }

  //连接zookeeper的zkUrl的url,判断该zookeeper上是否存在path路径
  def checkZkPathExists(zkUrl: String, path: String): Boolean = {
    try {
      val zk = new ZkClient(zkUrl, 30*1000,30*1000, ZKStringSerializer);
      zk.exists(path)
    } catch {
      case _: Throwable => false
    }
  }
}

trait MessageFormatter {
  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) //向output中写入key-value信息
  def init(props: Properties) {}
  def close() {}
}

/**
 * 向output中写入key-value信息,每一行信息写完后要添加换行符
 */
class DefaultMessageFormatter extends MessageFormatter {
  var printKey = false //信息中包含key
  var keySeparator = "\t".getBytes //向输出中写入key-value的拆分符
  var lineSeparator = "\n".getBytes //每一行的拆分符,用于向输出中写入换行符

  override def init(props: Properties) {
    if(props.containsKey("print.key"))
      printKey = props.getProperty("print.key").trim.toLowerCase.equals("true")
    if(props.containsKey("key.separator"))
      keySeparator = props.getProperty("key.separator").getBytes
    if(props.containsKey("line.separator"))
      lineSeparator = props.getProperty("line.separator").getBytes
  }

  //将key-value信息,经过处理写出到output流中
  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {
    if(printKey) {//包含key
      output.write(if (key == null) "null".getBytes() else key)//写入key的内容
      output.write(keySeparator)//写入key-value的分隔符
    }
    output.write(if (value == null) "null".getBytes() else value) //打印value,如果value是null,则打印null
    output.write(lineSeparator)//向输出中写入换行符
  }
}

//表示空实现,信息什么都不会输出
class NoOpMessageFormatter extends MessageFormatter {
  override def init(props: Properties) {}
  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {}
}

//表示输出该topic的校验和信息
class ChecksumMessageFormatter extends MessageFormatter {
  private var topicStr: String = _

  override def init(props: Properties) {
    topicStr = props.getProperty("topic")
    if (topicStr != null)
      topicStr = topicStr + ":"
    else
      topicStr = ""
  }

  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {
    val chksum = new Message(value, key).checksum //将key-value组装成message对象,并且计算key-value所有的字节数的校验和信息
    output.println(topicStr + "checksum:" + chksum) //输出该topic的校验和信息
  }
}
