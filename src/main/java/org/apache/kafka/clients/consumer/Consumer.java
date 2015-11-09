/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/
package org.apache.kafka.clients.consumer;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.MetricName;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K,V> extends Closeable {

    /**
     * Incrementally subscribe to the given list of topics. This API is mutually exclusive to 
     * {@link #subscribe(TopicPartition...) subscribe(partitions)} 
     * @param topics A variable list of topics that the consumer subscribes to
     * 增加订阅topic
     */ 
    public void subscribe(String...topics);

    /**
     * Incrementally subscribes to a specific topic and partition. This API is mutually exclusive to 
     * {@link #subscribe(String...) subscribe(topics)}
     * @param partitions Partitions to subscribe to
     * 增加订阅TopicPartition
     */ 
    public void subscribe(TopicPartition... partitions);

    /**
     * Unsubscribe from the specific topics. Messages for this topic will not be returned from the next {@link #poll(long) poll()}
     * onwards. This should be used in conjunction with {@link #subscribe(String...) subscribe(topics)}. It is an error to
     * unsubscribe from a topic that was never subscribed to using {@link #subscribe(String...) subscribe(topics)} 
     * @param topics Topics to unsubscribe from
     * 取消订阅topic,取消后在下一个poll投票的时候就不会从该topic中获取信息
     * 
     */
    public void unsubscribe(String... topics);

    /**
     * Unsubscribe from the specific topic partitions. Messages for these partitions will not be returned from the next 
     * {@link #poll(long) poll()} onwards. This should be used in conjunction with 
     * {@link #subscribe(TopicPartition...) subscribe(topic, partitions)}. It is an error to
     * unsubscribe from a partition that was never subscribed to using {@link #subscribe(TopicPartition...) subscribe(partitions)}
     * @param partitions Partitions to unsubscribe from
     * 取消订阅TopicPartition
     */
    public void unsubscribe(TopicPartition... partitions);
    
    /**
     * Fetches data for the subscribed list of topics and partitions
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available. If 0, waits indefinitely. Must not be negative
     *        超时时间,单位是毫秒,表示如果没有有效数据的时候,在抓取过程中要等候多久,如果该值是0,则表示等待时间不确定,该值不允许是负数
     * @return Map of topic to records for the subscribed topics and partitions as soon as data is available for a topic partition. Availability
     *         of data is controlled by {@link ConsumerConfig#FETCH_MIN_BYTES_CONFIG} and {@link ConsumerConfig#FETCH_MAX_WAIT_MS_CONFIG}.
     *         If no data is available for timeout ms, returns an empty list
     *         返回订阅的topics and partitions中有效的数据,key是topic,value是该topic的记录集合
     *         该数据抓取通过两个参数控制FETCH_MIN_BYTES_CONFIG、FETCH_MAX_WAIT_MS_CONFIG
     *         如果没有有效数据在单位时间内,则返回一个空的list集合
     *         
     *         
     * 从订阅的topics and partitions集合中抓取数据      
     */
    public Map<String, ConsumerRecords<K,V>> poll(long timeout);

    /**
     * Commits offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
     * @param sync If true, the commit should block until the consumer receives an acknowledgment 
     * @return An {@link OffsetMetadata} object that contains the partition, offset and a corresponding error code. Returns null
     * if the sync flag is set to false 
     * sync=false,则返回null
     * sync=true,则提交当前消费者消费到每一个TopicPartition的哪个偏移量了,提交给服务器
     */
    public OffsetMetadata commit(boolean sync);

    /**
     * Commits the specified offsets for the specified list of topics and partitions to Kafka.
     * @param offsets The map of offsets to commit for the given topic partitions
     * @param sync If true, commit will block until the consumer receives an acknowledgment 
     * @return An {@link OffsetMetadata} object that contains the partition, offset and a corresponding error code. Returns null
     * if the sync flag is set to false. 
     * sync=false,则返回null
     * sync=true,则提交当前消费者消费到每一个TopicPartition的哪个偏移量了,提交给服务器
     */
    public OffsetMetadata commit(Map<TopicPartition, Long> offsets, boolean sync);
    
    /**
     * Overrides the fetch positions that the consumer will use on the next fetch request. If the consumer subscribes to a list of topics
     * using {@link #subscribe(String...) subscribe(topics)}, an exception will be thrown if the specified topic partition is not owned by
     * the consumer.  
     * @param offsets The map of fetch positions per topic and partition
     * 重新定位每一个TopicPartition中下一次要获取message的偏移量,即重新定义每一个TopicPartition应该消费到哪里了
     */
    public void seek(Map<TopicPartition, Long> offsets);

    /**
     * Returns the fetch position of the <i>next message</i> for the specified topic partition to be used on the next {@link #poll(long) poll()}
     * @param partitions Partitions for which the fetch position will be returned
     * @return The position from which data will be fetched for the specified partition on the next {@link #poll(long) poll()}
     * 返回该消费者在每一个TopicPartition中下一次获取message信息的偏移量,即该TopicPartition已经消费到哪里了
     * 返回值key是TopicPartition,value是该TopicPartition中已经消费到哪里了
     */
    public Map<TopicPartition, Long> position(Collection<TopicPartition> partitions);
    
    /**
     * Fetches the last committed offsets for the input list of partitions 
     * @param partitions The list of partitions to return the last committed offset for
     * @return  The list of offsets for the specified list of partitions
     * 返回该消费者在每一个TopicPartition中最后提交的偏移量集合
     * 返回值key是TopicPartition,value是该TopicPartition中已经知道的最后已经提交的偏移量
     */
    public Map<TopicPartition, Long> committed(Collection<TopicPartition> partitions);
    
    /**
     * Fetches offsets before a certain timestamp
     * @param timestamp The unix timestamp. Value -1 indicates earliest available timestamp. Value -2 indicates latest available timestamp. 
     * @param partitions The list of partitions for which the offsets are returned
     * @return The offsets for messages that were written to the server before the specified timestamp.
     * 返回在timestamp之前最新的每一个TopicPartition消费到哪个偏移量位置了
     * 注意timestamp=-1表示获取第一次请求的偏移量位置,timestamp=-2表示最后一次请求的偏移量位置
     */
    public Map<TopicPartition, Long> offsetsBeforeTime(long timestamp, Collection<TopicPartition> partitions);

    /**
     * Return a map of metrics maintained by the consumer
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * Close this consumer
     */
    public void close();

}
