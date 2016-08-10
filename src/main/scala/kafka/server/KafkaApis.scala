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

import kafka.api._
import kafka.common._
import kafka.log._
import kafka.message._
import kafka.network._
import kafka.admin.AdminUtils
import kafka.network.RequestChannel.Response
import kafka.controller.KafkaController
import kafka.utils.{ZkUtils, ZKGroupTopicDirs, SystemTime, Logging}

import scala.collection._

import org.I0Itec.zkclient.ZkClient

/**
 * Logic to handle the various Kafka requests
 * 逻辑处理器,去处理各种各样的kafka请求
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val offsetManager: OffsetManager,
                val zkClient: ZkClient,
                val brokerId: Int,//在哪个节点上运行的该程序
                val config: KafkaConfig,
                val controller: KafkaController) extends Logging {

  val producerRequestPurgatory = new ProducerRequestPurgatory(replicaManager, offsetManager, requestChannel)
  val fetchRequestPurgatory = new FetchRequestPurgatory(replicaManager, requestChannel)
  // TODO: the following line will be removed in 0.9
  replicaManager.initWithRequestPurgatory(producerRequestPurgatory, fetchRequestPurgatory)
  var metadataCache = new MetadataCache//缓存服务
  //日志的唯一标识,使用brokerId进行唯一标识
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try{
      //为远程客户端ip发过来的request请求,进行处理
      trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress)
      request.requestId match {
        case RequestKeys.ProduceKey => handleProducerOrOffsetCommitRequest(request) //处理生产者发到该broker的请求
        case RequestKeys.ConsumerMetadataKey => handleConsumerMetadataRequest(request)//获取制定group所在的offset的topic所在partition的leader节点
        
        case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)//获取TopicMetadata元数据信息

        //controller相关的请求
        case RequestKeys.UpdateMetadataKey => handleUpdateMetadataRequest(request)//更新每一个partition的详细信息元数据
        case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request)
        case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
        case RequestKeys.ControlledShutdownKey => handleControlledShutdownRequest(request)
        
        case RequestKeys.OffsetsKey => handleOffsetRequest(request)//获取topic-partition当前的offset偏移量
        case RequestKeys.OffsetCommitKey => handleOffsetCommitRequest(request) //更新topic-partition的offset请求,版本号0的时候走一套老逻辑,版本号>0,则就当生产者请求处理,向特定topic存储该信息
        
        case RequestKeys.FetchKey => handleFetchRequest(request)//key表示抓取哪个topic-partition数据,value表示从offset开始抓取,抓取多少个数据返回
        case RequestKeys.OffsetFetchKey => handleOffsetFetchRequest(request)//抓去每一个topic-partition对应的offset最新信息
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        request.requestObj.handleError(e, requestChannel, request)
        error("error when handling request %s".format(request.requestObj), e)
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  //更新topic-partition的offset请求,版本号0的时候走一套老逻辑,版本号>0,则就当生产者请求处理,向特定topic存储该信息
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val offsetCommitRequest = request.requestObj.asInstanceOf[OffsetCommitRequest] //对象转换
    if (offsetCommitRequest.versionId == 0) {//如果是版本号0的时候,即老版本
      // version 0 stores the offsets in ZK 版本号0的时候,不存储在特定的topic中,而是存储在zookeeper中
      val responseInfo = offsetCommitRequest.requestInfo.map{
        case (topicAndPartition, metaAndError) => {
          val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicAndPartition.topic)
          try {
            ensureTopicExists(topicAndPartition.topic)//确保topic是存在的
            if(metaAndError.metadata != null && metaAndError.metadata.length > config.offsetMetadataMaxSize) {//信息不允许>设定的最大长度
              (topicAndPartition, ErrorMapping.OffsetMetadataTooLargeCode)//异常,说message信息太长
            } else {
              ///consumers/${group}/offsets/${topic}/${partition} 内容是该topic-partition对应存储的offset,版本1之后,已经不存储在该节点上了,存储在特定的topic上
              ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" +
                topicAndPartition.partition, metaAndError.offset.toString)
              (topicAndPartition, ErrorMapping.NoError)
            }
          } catch {
            case e: Throwable => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
          }
        }
      }
      val response = new OffsetCommitResponse(responseInfo, offsetCommitRequest.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    } else {//新版本,版本号>0,则说明该请求就是一个offset请求,转换成生产者请求即可
      // version 1 and above store the offsets in a special Kafka topic
      handleProducerOrOffsetCommitRequest(request)
    }
  }

  /**
   * 确保topic是存在的
   */
  private def ensureTopicExists(topic: String) = {
    if (metadataCache.getTopicMetadata(Set(topic)).size <= 0)
      throw new UnknownTopicOrPartitionException("Topic " + topic + " either doesn't exist or is in the process of being deleted")//topic不存在,或者在该进程中被删除
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val leaderAndIsrRequest = request.requestObj.asInstanceOf[LeaderAndIsrRequest] //反序列化controller发过来的partition的leader详细信息和备份节点集合
    try {
      val (response, error) = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest, offsetManager)
      val leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, response, error)
      requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.requestObj.asInstanceOf[StopReplicaRequest]
    val (response, error) = replicaManager.stopReplicas(stopReplicaRequest)
    val stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, response.toMap, error)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(stopReplicaResponse)))
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val updateMetadataRequest = request.requestObj.asInstanceOf[UpdateMetadataRequest]
    replicaManager.maybeUpdateMetadataCache(updateMetadataRequest, metadataCache)

    val updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(updateMetadataResponse)))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]
    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId) //该节点仍然是leader的partition集合
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      ErrorMapping.NoError, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(controlledShutdownResponse)))
  }

  /**
   * 1.对offset中描述信息超过一定数量的过滤掉
   * 2.对过滤后的合法数据,转换成message字节数组
   * 
   * 该方法主要是对offset请求的转换,转换成一个生产者请求
   */
  private def producerRequestFromOffsetCommit(offsetCommitRequest: OffsetCommitRequest) = {
    val msgs = offsetCommitRequest.filterLargeMetadata(config.offsetMetadataMaxSize).map {
      case (topicAndPartition, offset) =>
        new Message(
          bytes = OffsetManager.offsetCommitValue(offset),//将偏移量信息转换成字节数组
          key = OffsetManager.offsetCommitKey(offsetCommitRequest.groupId, topicAndPartition.topic, topicAndPartition.partition) //将group-topic-partition组装成key字节数组
        )
    }.toSeq

    //找到__consumer_offsets--该group对应的partition,以及要向他们发送的信息
    val producerData = mutable.Map(
      TopicAndPartition(OffsetManager.OffsetsTopicName, offsetManager.partitionFor(offsetCommitRequest.groupId)) ->
        new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, msgs:_*)
    )

    //生产者请求
    val request = ProducerRequest(
      correlationId = offsetCommitRequest.correlationId,
      clientId = offsetCommitRequest.clientId,
      requiredAcks = config.offsetCommitRequiredAcks,
      ackTimeoutMs = config.offsetCommitTimeoutMs,
      data = producerData)
    trace("Created producer request %s for offset commit request %s.".format(request, offsetCommitRequest))
    request
  }

  /**
   * Handle a produce request or offset commit request (which is really a specialized producer request)
   * 返回值是 produceRequest, offsetCommitRequestOpt,key是生产者发过来的请求或者提交OffsetCommitRequest请求,value分别是none和OffsetCommitRequest对象
   */
  def handleProducerOrOffsetCommitRequest(request: RequestChannel.Request) {
    val (produceRequest, offsetCommitRequestOpt) =
      if (request.requestId == RequestKeys.OffsetCommitKey) {//说明是生产者提交的已经消费到哪个序号请求
        val offsetCommitRequest = request.requestObj.asInstanceOf[OffsetCommitRequest]
        OffsetCommitRequest.changeInvalidTimeToCurrentTime(offsetCommitRequest)//填充非法的时间戳
        (producerRequestFromOffsetCommit(offsetCommitRequest), Some(offsetCommitRequest)) //对offset请求的转换,转换成一个生产者请求
      } else {//生产者提交的请求
        (request.requestObj.asInstanceOf[ProducerRequest], None)
      }

    //只有三个值,-1 0 1,因此超过范围的说明有问题,打印异常
    if (produceRequest.requiredAcks > 1 || produceRequest.requiredAcks < -1) {//校验回执是否设置正确
      warn(("Client %s from %s sent a produce request with request.required.acks of %d, which is now deprecated and will " +
            "be removed in next release. Valid values are -1, 0 or 1. Please consult Kafka documentation for supported " +
            "and recommended configuration.").format(produceRequest.clientId, request.remoteAddress, produceRequest.requiredAcks))
    }

    val sTime = SystemTime.milliseconds
    val localProduceResults = appendToLocalLog(produceRequest, offsetCommitRequestOpt.nonEmpty) //向本地的partition中写入数据,返回Iterable[ProduceResult]结果集
    debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime)) //打印写入log花费时间

    //返回有异常的状态码
    val firstErrorCode = localProduceResults.find(_.errorCode != ErrorMapping.NoError).map(_.errorCode).getOrElse(ErrorMapping.NoError)

    //记录多少个partition写入时候有异常
    val numPartitionsInError = localProduceResults.count(_.error.isDefined)
    if(produceRequest.requiredAcks == 0) {
      // no operation needed if producer request.required.acks = 0; however, if there is any exception in handling the request, since
      // no response is expected by the producer the handler will send a close connection response to the socket server
      // to close the socket so that the producer client will know that some exception has happened and will refresh its metadata
      if (numPartitionsInError != 0) { //有异常信息,因此发送一个关闭连接的回复即可
        info(("Send the close connection response due to error handling produce request " +
          "[clientId = %s, correlationId = %s, topicAndPartition = %s] with Ack=0")
          .format(produceRequest.clientId, produceRequest.correlationId, produceRequest.topicPartitionMessageSizeMap.keySet.mkString(",")))
        requestChannel.closeConnection(request.processor, request)
      } else {//说明没有任何异常

        if (firstErrorCode == ErrorMapping.NoError) //更新offset的缓存信息
          offsetCommitRequestOpt.foreach(ocr => offsetManager.putOffsets(ocr.groupId, ocr.requestInfo))

        if (offsetCommitRequestOpt.isDefined) {//说明是offset生产者
          val response = offsetCommitRequestOpt.get.responseFor(firstErrorCode, config.offsetMetadataMaxSize)
          requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))//返回状态信息
        } else //说明是普通生产者
          requestChannel.noOperation(request.processor, request)
      }
    } else if (produceRequest.requiredAcks == 1 ||
        produceRequest.numPartitions <= 0 ||
        numPartitionsInError == produceRequest.numPartitions) {

      if (firstErrorCode == ErrorMapping.NoError) {
        offsetCommitRequestOpt.foreach(ocr => offsetManager.putOffsets(ocr.groupId, ocr.requestInfo) )
      }

      val statuses = localProduceResults.map(r => r.key -> ProducerResponseStatus(r.errorCode, r.start)).toMap
      val response = offsetCommitRequestOpt.map(_.responseFor(firstErrorCode, config.offsetMetadataMaxSize))
                                           .getOrElse(ProducerResponse(produceRequest.correlationId, statuses))

      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    } else {
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val producerRequestKeys = produceRequest.data.keys.toSeq //TopicAndPartition集合
      val statuses = localProduceResults.map(r =>
        r.key -> DelayedProduceResponseStatus(r.end + 1, ProducerResponseStatus(r.errorCode, r.start))).toMap
      val delayedRequest =  new DelayedProduce(
        producerRequestKeys,
        request,
        produceRequest.ackTimeoutMs.toLong,
        produceRequest,
        statuses,
        offsetCommitRequestOpt)

      // add the produce request for watch if it's not satisfied, otherwise send the response back
      val satisfiedByMe = producerRequestPurgatory.checkAndMaybeWatch(delayedRequest)
      if (satisfiedByMe)
        producerRequestPurgatory.respond(delayedRequest)
    }

    // we do not need the data anymore 我们不在需要该数据了,则清空数据内容
    produceRequest.emptyData()
  }

  //生产者的结果,某个topic-partition的message的下标从什么到什么
  case class ProduceResult(key: TopicAndPartition, start: Long, end: Long, error: Option[Throwable] = None) {
    def this(key: TopicAndPartition, throwable: Throwable) = 
      this(key, -1L, -1L, Some(throwable))
    
    def errorCode = error match {
      case None => ErrorMapping.NoError
      case Some(error) => ErrorMapping.codeFor(error.getClass.asInstanceOf[Class[Throwable]])
    }
  }

  /**
   * Helper method for handling a parsed producer request
   * @producerRequest 表示生产者的请求
   * @isOffsetCommit 因为生产者的请求可能是offset请求,true则表示该生产者是offset请求,发往指定的内部topic信息
   * 将生产者的信息追加到本地的log中
   */
  private def appendToLocalLog(producerRequest: ProducerRequest, isOffsetCommit: Boolean): Iterable[ProduceResult] = {
    val partitionAndData: Map[TopicAndPartition, MessageSet] = producerRequest.data //生产者向什么topic-partition发送了哪些信息的映射关系
    trace("Append [%s] to local log ".format(partitionAndData.toString)) //追加本地log中
    partitionAndData.map {case (topicAndPartition, messages) =>
      try {
        //校验,不允许追加kafka内部的topic,即__consumer_offsets,并且isOffsetCommit=true与__consumer_offsets是topic相对应
        if (Topic.InternalTopics.contains(topicAndPartition.topic) &&
            !(isOffsetCommit && topicAndPartition.topic == OffsetManager.OffsetsTopicName)) {
          throw new InvalidTopicException("Cannot append to internal topic %s".format(topicAndPartition.topic))
        }
        //获取对应的partition对象
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val info = partitionOpt match {
          case Some(partition) =>
            partition.appendMessagesToLeader(messages.asInstanceOf[ByteBufferMessageSet],producerRequest.requiredAcks) //向本地日志中写入生产者发来的信息
          case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
            .format(topicAndPartition, brokerId)) //打印日志,说明该partition不再该brokerId上
        }

        //显示该partition目前已经追加了多少个message了
        val numAppendedMessages = if (info.firstOffset == -1L || info.lastOffset == -1L) 0 else (info.lastOffset - info.firstOffset + 1)

        // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesInRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).messagesInRate.mark(numAppendedMessages)
        BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)

        trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
              .format(messages.size, topicAndPartition.topic, topicAndPartition.partition, info.firstOffset, info.lastOffset))
        ProduceResult(topicAndPartition, info.firstOffset, info.lastOffset)
      } catch {
        // NOTE: Failed produce requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
        // since failed produce requests metric is supposed to indicate failure of a broker in handling a produce request
        // for a partition it is the leader for
        case e: KafkaStorageException =>
          fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
          Runtime.getRuntime.halt(1)
          null
        case ite: InvalidTopicException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            producerRequest.correlationId, producerRequest.clientId, topicAndPartition, ite.getMessage))
          new ProduceResult(topicAndPartition, ite)
        case utpe: UnknownTopicOrPartitionException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
               producerRequest.correlationId, producerRequest.clientId, topicAndPartition, utpe.getMessage))
          new ProduceResult(topicAndPartition, utpe)
        case nle: NotLeaderForPartitionException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
               producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nle.getMessage))
          new ProduceResult(topicAndPartition, nle)
        case nere: NotEnoughReplicasException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nere.getMessage))
          new ProduceResult(topicAndPartition, nere)
        case e: Throwable =>
          BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).failedProduceRequestRate.mark()
          BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
          error("Error processing ProducerRequest with correlation id %d from client %s on partition %s"
            .format(producerRequest.correlationId, producerRequest.clientId, topicAndPartition), e)
          new ProduceResult(topicAndPartition, e)
       }
    }
  }

  /**
   * Handle a fetch request
   * key表示抓取哪个topic-partition数据,value表示从offset开始抓取,抓取多少个数据返回
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]
    //返回值Map[TopicAndPartition, PartitionDataAndOffset],即在topic-partition 抓去了哪些信息
    val dataRead = replicaManager.readMessageSets(fetchRequest)

    // if the fetch request comes from the follower,
    // update its corresponding log end offset
    //如果该请求来自于follower节点,则更新操作
    if(fetchRequest.isFromFollower)
      //_.offset是PartitionDataAndOffset.offset,即LogOffsetMetadata对象
      recordFollowerLogEndOffsets(fetchRequest.replicaId, dataRead.mapValues(_.offset))//记录该follower节点replicaId,已经同步给他了每一个topic-partition到哪个offset了

    // check if this fetch request can be satisfied right away
    val bytesReadable = dataRead.values.map(_.data.messages.sizeInBytes).sum//本次读取了多少个字节的数据
    //从左计算到右边,默认是false,查看是否有error信息
    val errorReadingData = dataRead.values.foldLeft(false)((errorIncurred, dataAndOffset) =>
      errorIncurred || (dataAndOffset.data.error != ErrorMapping.NoError))
    // send the data immediately 如果满足以下四个条件的,则数据立即发送到客户端,即 例如follower节点
    //                           if 1) fetch request does not want to wait 抓去的请求不需要等待,抓去到了,就发送
    //                              2) fetch request does not require any data 抓去请求中没有任何要抓去的partition请求
    //                              3) has enough data to respond 已经有足够多的数据了,可以发送回去了
    //                              4) some error happens while reading data 在读数据的过程中,有一些异常发生了,也要立即发送出去
    if(fetchRequest.maxWait <= 0 ||
       fetchRequest.numPartitions <= 0 || //没有partition要去抓去
       bytesReadable >= fetchRequest.minBytes ||//说明读取的数据量已经够多了
       errorReadingData) {//有异常
      debug("Returning fetch response %s for fetch request with correlation id %d to client %s"
        .format(dataRead.values.map(_.data.error).mkString(","), fetchRequest.correlationId, fetchRequest.clientId))
      val response = new FetchResponse(fetchRequest.correlationId, dataRead.mapValues(_.data))//把数据返回给客户端
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    } else {//说明暂时不发送数据给客户端,要存放一阵子
      debug("Putting fetch request with correlation id %d from client %s into purgatory".format(fetchRequest.correlationId,
        fetchRequest.clientId))
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val delayedFetchKeys = fetchRequest.requestInfo.keys.toSeq
      val delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest.maxWait, fetchRequest,
        dataRead.mapValues(_.offset))

      // add the fetch request for watch if it's not satisfied, otherwise send the response back
      val satisfiedByMe = fetchRequestPurgatory.checkAndMaybeWatch(delayedFetch)
      if (satisfiedByMe)
        fetchRequestPurgatory.respond(delayedFetch)
    }
  }

  //记录该follower节点replicaId,已经同步给他了每一个topic-partition到哪个offset了
  private def recordFollowerLogEndOffsets(replicaId: Int, offsets: Map[TopicAndPartition, LogOffsetMetadata]) {
    debug("Record follower log end offsets: %s ".format(offsets))
    offsets.foreach {
      case (topicAndPartition, offset) =>
        replicaManager.updateReplicaLEOAndPartitionHW(topicAndPartition.topic,
          topicAndPartition.partition, replicaId, offset)

        // for producer requests with ack > 1, we need to check
        // if they can be unblocked after some follower's log end offsets have moved
        replicaManager.unblockDelayedProduceRequests(topicAndPartition)
    }
  }

  /**
   * Service the offset request API 
   * 获取topic-partition符合条件的logSegment文件编号
   */
  def handleOffsetRequest(request: RequestChannel.Request) {
    val offsetRequest = request.requestObj.asInstanceOf[OffsetRequest]
    //offsetRequest.requestInfo表示TopicAndPartition, PartitionOffsetRequestInfo
    val responseMap = offsetRequest.requestInfo.map(elem => {
      val (topicAndPartition, partitionOffsetRequestInfo) = elem
      try {
        // ensure leader exists
        val localReplica = if(!offsetRequest.isFromDebuggingClient)
          replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
        else
          replicaManager.getReplicaOrException(topicAndPartition.topic, topicAndPartition.partition)
        val offsets = {
          val allOffsets = fetchOffsets(replicaManager.logManager,
                                        topicAndPartition,
                                        partitionOffsetRequestInfo.time,
                                        partitionOffsetRequestInfo.maxNumOffsets)
          if (!offsetRequest.isFromOrdinaryClient) {
            allOffsets
          } else {
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(_ > hw))//查找>hw的序号
              hw +: allOffsets.dropWhile(_ > hw)
            else 
              allOffsets
          }
        }
        (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          warn("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case nle: NotLeaderForPartitionException =>
          warn("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition,nle.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case e: Throwable =>
          warn("Error while responding to offset request", e)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), Nil) )
      }
    })
    val response = OffsetResponse(offsetRequest.correlationId, responseMap)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }
  
  /**
   * 抓去topic-partition的logSegments日志的起始编号
   * 1.必须该文件是在本地系统中
   * 2.抓去条件是日志的修改时间必须是timestamp之前写进去的
   * 3.最多抓去maxNumOffsets个logSegments日志的起始编号
   */
  def fetchOffsets(logManager: LogManager, topicAndPartition: TopicAndPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    logManager.getLog(topicAndPartition) match {
      case Some(log) => 
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None => 
        if (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime)
          Seq(0L)
        else
          Nil
    }
  }
  
  /**
   * 抓去log日志的logSegments的起始编号,最多抓去maxNumOffsets个logSegments日志的起始编号
   * 抓去条件是日志的修改时间必须是timestamp之前写进去的
   */
  def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val segsArray = log.logSegments.toArray//记录该log所有的logSegments文件
    var offsetTimeArray: Array[(Long, Long)] = null //设置该log所有的logSegments文件对应的起始编号以及文件最后修改时间
    if(segsArray.last.size > 0)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    for(i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
    if(segsArray.last.size > 0)
      offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds) //记录当前文件的正在写的编号以及当前时间

    var startIndex = -1//从什么索引开始查找数据
    timestamp match {
      case OffsetRequest.LatestTime =>
        startIndex = offsetTimeArray.length - 1
      case OffsetRequest.EarliestTime =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {//从后往前查找,直到找到了位置
          if (offsetTimeArray(startIndex)._2 <= timestamp) //找到时间戳之前存储的文件
            isFound = true
          else
            startIndex -=1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)//maxNumOffsets与startIndex + 1取最小的值
    val ret = new Array[Long](retSize)
    for(j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1//设置该文件的起始编号
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(- _)
  }

  /**
   * 根据topic集合,返回该集合对应的元数据信息集合
   */
  private def getTopicMetadata(topics: Set[String]): Seq[TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics) //获取topic的元数据信息
    if (topics.size > 0 && topicResponses.size != topics.size) {//说明请求的topic和返回的topic数量不一致
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet//找到差异在哪些topic上
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (topic == OffsetManager.OffsetsTopicName || config.autoCreateTopicsEnable) {//自动创建该topic
          try {
            if (topic == OffsetManager.OffsetsTopicName) {//topic是内部定义的offset所属的topic
              val aliveBrokers = metadataCache.getAliveBrokers
              val offsetsTopicReplicationFactor =
                if (aliveBrokers.length > 0)
                  Math.min(config.offsetsTopicReplicationFactor, aliveBrokers.length)
                else
                  config.offsetsTopicReplicationFactor
              AdminUtils.createTopic(zkClient, topic, config.offsetsTopicPartitions,
                                     offsetsTopicReplicationFactor,
                                     offsetManager.offsetsTopicConfig)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                .format(topic, config.offsetsTopicPartitions, offsetsTopicReplicationFactor))
            }
            else {//说明是自动创建该topic
              AdminUtils.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                   .format(topic, config.numPartitions, config.defaultReplicationFactor))
            }
          } catch {
            case e: TopicExistsException => // let it go, possibly another broker created this topic
          }
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.LeaderNotAvailableCode) //leader不可用
        } else {
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)//报告缺失该topic信息
        }
      }
      topicResponses.appendAll(responsesForNonExistentTopics)
    }
    topicResponses
  }

  /**
   * Service the topic metadata request API
   * 获取TopicMetadata元数据信息
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]
    val topicMetadata = getTopicMetadata(metadataRequest.topics.toSet)//获取topic对应的元数据信息集合
    val brokers = metadataCache.getAliveBrokers//返回Seq[Broker],所有的节点集合
    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(topicMetadata.mkString(","), brokers.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
    val response = new TopicMetadataResponse(brokers, topicMetadata, metadataRequest.correlationId)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  /*
   * Service the Offset fetch API
   * 抓去每一个topic-partition对应的offset最新信息
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val offsetFetchRequest = request.requestObj.asInstanceOf[OffsetFetchRequest]

    if (offsetFetchRequest.versionId == 0) {//从zookeeper中获取offset信息
      // version 0 reads offsets from ZK
      val responseInfo = offsetFetchRequest.requestInfo.map( t => {
        val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, t.topic)
        try {
          ensureTopicExists(t.topic)
          val payloadOpt = ZkUtils.readDataMaybeNull(zkClient, topicDirs.consumerOffsetDir + "/" + t.partition)._1
          payloadOpt match {
            case Some(payload) => {
              (t, OffsetMetadataAndError(offset=payload.toLong, error=ErrorMapping.NoError))
            }
            case None => (t, OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
              ErrorMapping.UnknownTopicOrPartitionCode))
          }
        } catch {
          case e: Throwable =>
            (t, OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
              ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])))
        }
      })
      val response = new OffsetFetchResponse(collection.immutable.Map(responseInfo: _*), offsetFetchRequest.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    } else {//从kafka的特定topic中获取offset信息
      // version 1 reads offsets from Kafka按照是否缓存中存在topic-partition进行拆分,成两个集合,一个是不存在的集合,一个是存在的集合
      val (unknownTopicPartitions, knownTopicPartitions) = offsetFetchRequest.requestInfo.partition(topicAndPartition =>
        metadataCache.getPartitionInfo(topicAndPartition.topic, topicAndPartition.partition).isEmpty
      )
      //将不存在的topic-partition改成Map[TopicAndPartition, OffsetMetadataAndError],其中异常原因是不存在该topic-partition
      val unknownStatus = unknownTopicPartitions.map(topicAndPartition => (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)).toMap
      // Map[TopicAndPartition, OffsetMetadataAndError]将存在的topic-partition查找他的offset信息
      val knownStatus =
        if (knownTopicPartitions.size > 0)
          offsetManager.getOffsets(offsetFetchRequest.groupId, knownTopicPartitions).toMap
        else
          Map.empty[TopicAndPartition, OffsetMetadataAndError]//说明没有topic-partition信息
      val status = unknownStatus ++ knownStatus//汇总集合

      val response = OffsetFetchResponse(status, offsetFetchRequest.correlationId)

      trace("Sending offset fetch response %s for correlation id %d to client %s."
            .format(response, offsetFetchRequest.correlationId, offsetFetchRequest.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    }
  }

  /*
   * Service the consumer metadata API
   * 获取制定group所在的offset的topic所在partition的leader节点
   */
  def handleConsumerMetadataRequest(request: RequestChannel.Request) {
    val consumerMetadataRequest = request.requestObj.asInstanceOf[ConsumerMetadataRequest]

    val partition = offsetManager.partitionFor(consumerMetadataRequest.group)//在哪个partition中

    // get metadata (and create the topic if necessary) 获取offset的topic的元数据
    val offsetsTopicMetadata = getTopicMetadata(Set(OffsetManager.OffsetsTopicName)).head

    //默认返回值
    val errorResponse = ConsumerMetadataResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode, consumerMetadataRequest.correlationId)

    /**
     * 1.查找指定partition的元数据PartitionMetadata
     * 2.找到该partition对应的leader节点
     */
    val response =
      offsetsTopicMetadata.partitionsMetadata.find(_.partitionId == partition).map { partitionMetadata =>
        partitionMetadata.leader.map { leader =>
          ConsumerMetadataResponse(Some(leader), ErrorMapping.NoError, consumerMetadataRequest.correlationId)
        }.getOrElse(errorResponse)
      }.getOrElse(errorResponse)

    trace("Sending consumer metadata %s for correlation id %d to client %s."
          .format(response, consumerMetadataRequest.correlationId, consumerMetadataRequest.clientId))
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  def close() {
    debug("Shutting down.")
    fetchRequestPurgatory.shutdown()
    producerRequestPurgatory.shutdown()
    debug("Shut down complete.")
  }
}

