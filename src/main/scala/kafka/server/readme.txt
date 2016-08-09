一、KafkaServer
 代表一个kafka的单节点,该类控制该节点的生命周期
 处理该节点的启动和关闭服务功能

是一个服务器节点的入口,是通过kafka.Kafka 是整个程序的入口类调用的。

二、需要哪些服务
1.连接zookeeper
2.LogManager 用于在该节点存储topic-partition的日志服务
3.new SocketServer在该节点开通一个服务端socket,用于接收各种服务命令
4.new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads) 处理socket的请求,从该socketserver中不断拿到request请求去处理
5.KafkaHealthcheck 用于将该broker节点的信息注册到zookeeper中,并且当节点失效的时候,继续重新注册到zookeeper的过程。
6.TopicConfigManager 当topic的配置文件发生变化的时候,要更新LogManager中每一个Topic-partition对应的配置对象
7.ReplicaManager
8.OffsetManager 用于存储group-topic-partition的offset信息的配置 ,即每一个消费组消费每一个topic-partition到哪个序号了
                只有存储该topic的log节点上或者备份节点上有该对象,但是每一个节点都会产生一个该对象,只是可能该对象没有意义
9.KafkaController


三、整体流程
1.用户先产生一个topic,协商好要拥有几个partition,以及备份因子
2.程序会自动产生每一个partition的备份节点在哪里,并且将其写入到zookeeper的topic中
3.当controller启动的时候,会加载所有的topic内容,因此就知道topic-partition-备份节点集合之间的关系了
4.当集群运行中,新产生一个topic,因为controller.PartitionStateMachine监听了/brokers/topics,因此会通知controller创建该topic以及读取zookeeper,创建对应的partition对象,添加到zookeeper映射中。
此时partition是new状态。同时也会产生备份对象,会经过ReplicaStateMachine状态机处理



未解之谜
partition的getOrCreateReplica方法,为什么要与log.logEndOffset的进行比较,获取较小的值,创建备份对象

因为topic的partition在哪些节点上,zookeeper上是有记录的
因此在这些节点上才能找到对应的partition