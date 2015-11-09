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
package org.apache.kafka.clients.producer.internals;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class encapsulating some of the logic around metadata.
 * 该类封装了一些元数据
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 * 
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metdata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * Metadata顾名思义保存的是kafka集群的元数据，metadata的更新和topic有关。
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    private final long refreshBackoffMs;//最小刷新时间,即使metadataExpireMs设置的再小,也不能小于refreshBackoffMs进行刷新
    private final long metadataExpireMs;//元数据过期周期
    private int version;//当前版本
    private long lastRefreshMs;//最后一次刷新时间
    private Cluster cluster;
    private boolean needUpdate;//是否需要更新,true表示要请求更新元数据信息
    private final Set<String> topics;//管理topic集合

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);//60 * 60 * 1000L = 一小时
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh 不用刷新,可以再这个期间内保持元数据信息
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashSet<String>();
    }

    /**
     * Get the current cluster info without blocking
     * 获取当前集群对象
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata
     */
    public synchronized void add(String topic) {
        topics.add(topic);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire
     * and the time the current info can be updated (i.e. backoff time has elapsed); If an update has
     * been request then the expiry time is now
     * 根据当前时间,返回还需要多久才能更新元数据
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     * 请求一个更新集群元数据请求,并且返回请求前的版本号
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     * 等候,直到当前的版本version 大于 参数lastVerison为止,或者时间超时为止
     */
    public synchronized void awaitUpdate(int lastVerison, long maxWaitMs) {
        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        while (this.version <= lastVerison) {
            try {
                wait(remainingWaitMs);
            } catch (InterruptedException e) { /* this is fine */
            }
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }
    
    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<String>(this.topics);
    }

    /**
     * Update the cluster metadata
     */
    public synchronized void update(Cluster cluster, long now) {
        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.version += 1;
        this.cluster = cluster;
        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * The last time metadata was updated.
     */
    public synchronized long lastUpdate() {
        return this.lastRefreshMs;
    }

    /**
     * The metadata refresh backoff in ms
     */
    public long refreshBackoff() {
        return refreshBackoffMs;
    }
}
