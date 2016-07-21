一、KafkaServer
 代表一个kafka的单节点,该类控制该节点的生命周期
 处理该节点的启动和关闭服务功能

是一个服务器节点的入口,是通过kafka.Kafka 是整个程序的入口类调用的。

二、需要哪些服务
1.连接zookeeper
2.LogManager 用于在该节点存储topic-partition的日志服务
3.new SocketServer在该节点开通一个服务端socket,用于接收各种服务命令
4.new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads) 处理socket的请求
5.KafkaHealthcheck 用于将该broker节点的信息注册到zookeeper中,并且当节点失效的时候,继续重新注册到zookeeper的过程。
6.TopicConfigManager 当topic的配置文件发生变化的时候,要更新LogManager中每一个Topic-partition对应的配置对象
7.ReplicaManager
8.OffsetManager
9.KafkaController


未解之谜
1.kafka的基础是partition
2.为了防止partition在本机不可用,则设置了备份
3.一个备份是Replica
4.一个partition的备份管理由ReplicaManager管理

他们之间是怎么个关系呢？
有点晕


因为topic的partition在哪些节点上,zookeeper上是有记录的
因此在这些节点上才能找到对应的partition