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
package org.apache.kafka.clients.producer.internals;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;


/**
 * The default partitioning strategy:
 * 默认的Partitione策略
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 * 理论上ProducerRecord包含key-value,以及要将其存储到哪个topic和partition里面
 * 
 * 当partition指定了,则就将key-value其存储到该partition中即可
 * 当partition没有被指定,则基于key进行hash,找到对应的partition即可
 * 当partition没有被指定,key=null,则用自增长作为key,对partition去摩
 */
public class Partitioner {

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    /**
     * Compute the partition for the given record.
     * 
     * @param record The record being sent
     * @param cluster The current cluster metadata
     * 根据策略返回该ProducerRecord要存储到第几个partition上
     */
    public int partition(ProducerRecord<byte[], byte[]> record, Cluster cluster) {
    	//通过topic获取该topic对应的partition集合
        List<PartitionInfo> partitions = cluster.partitionsForTopic(record.topic());
        
        int numPartitions = partitions.size();
        if (record.partition() != null) {//当partition指定了,则就将key-value其存储到该partition中即可
            // they have given us a partition, use it
            if (record.partition() < 0 || record.partition() >= numPartitions)
                throw new IllegalArgumentException("Invalid partition given with record: " + record.partition()
                                                   + " is not in the range [0..."
                                                   + numPartitions
                                                   + "].");//校验partition的合法性
            return record.partition();
        } else if (record.key() == null) {
            int nextValue = counter.getAndIncrement();
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(record.topic());
            if (availablePartitions.size() > 0) {
                int part = Utils.abs(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.abs(nextValue) % numPartitions;
            }
        } else {
            // hash the key to choose a partition
            return Utils.abs(Utils.murmur2(record.key())) % numPartitions;
        }
    }

}
