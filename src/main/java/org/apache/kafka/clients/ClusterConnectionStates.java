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
package org.apache.kafka.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * The state of our connection to each node in the cluster.
 * 管理连接集群每一个节点的状态对象
 */
final class ClusterConnectionStates {
	
    private final long reconnectBackoffMs;//多少毫秒后要进行重新连接
    
    //key表示节点编码,value:表示该客户端与该节点的连接状态与最后连接时间
    private final Map<Integer, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.nodeState = new HashMap<Integer, NodeConnectionState>();
    }

    /**
     * Return true iff we can currently initiate a new connection to the given node. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param node The node id to check
     * @param now The current time in MS 当前连接的时间
     * @return true if we can initiate a new connection
     * true表示要初始化一个新的链接到给定的node节点上
     * true的条件是我们目前尚未与该node有链接,或者我们与该node的链接是断开的,并且断开时间已经大于重新连接时间阀值,则要进行重新连接
     */
    public boolean canConnect(int node, long now) {
        NodeConnectionState state = nodeState.get(node);
        if (state == null)
            return true;
        else
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs >= this.reconnectBackoffMs;
            //注意now - state.lastConnectAttemptMs表示当前时间与最后与该node连接的时间差,即已经停止多久了,如果大于重新连接阀值reconnectBackoffMs,则表示可以进行重新连接
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection yet
     * @param node The node to check
     * @param now The current time in ms
     * true表示与该node必须是DISCONNECTED,并且目前仍然不能重新建立连接
     */
    public boolean isBlackedOut(int node, long now) {
        NodeConnectionState state = nodeState.get(node);
        if (state == null)
            return false;
        else
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs < this.reconnectBackoffMs;
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * 在尝试发送数据给该节点之前,需要等待多少毫秒
     * 
     * @param node The node to check
     * @param now The current time in ms
     */
    public long connectionDelay(int node, long now) {
        NodeConnectionState state = nodeState.get(node);
        if (state == null) return 0;//如果没有与该节点连接过,则返回0,表示不需要等待
        long timeWaited = now - state.lastConnectAttemptMs;
        if (state.state == ConnectionState.DISCONNECTED) {//必须等待一个重新连接阀值,超过该阀值,则会返回0,表示不需要等待
            return Math.max(this.reconnectBackoffMs - timeWaited, 0);
        } else {
            // When connecting or connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
        	//如果已经连接中或者连接成功了,我们应该无限制的延期,因为其他时间可能已经发送数据了
            return Long.MAX_VALUE;
        }
    }

    /**
     * Enter the connecting state for the given node.
     * @param node The id of the node we are connecting to
     * @param now The current time.
     * 设置与该node要进行连接中状态
     */
    public void connecting(int node, long now) {
        nodeState.put(node, new NodeConnectionState(ConnectionState.CONNECTING, now));
    }

    /**
     * Return true iff we have a connection to the give node
     * @param node The id of the node to check
     * 检查是否与该节点已经连接了
     */
    public boolean isConnected(int node) {
        NodeConnectionState state = nodeState.get(node);
        return state != null && state.state == ConnectionState.CONNECTED;
    }

    /**
     * Return true iff we are in the process of connecting to the given node
     * @param node The id of the node
     * 检查是否与该节点正在连接中
     */
    public boolean isConnecting(int node) {
        NodeConnectionState state = nodeState.get(node);
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * Enter the connected state for the given node
     * @param node The node we have connected to
     * 设置当前与node节点的链接为连接成功
     */
    public void connected(int node) {
        nodeState(node).state = ConnectionState.CONNECTED;
    }

    /**
     * Enter the disconnected state for the given node
     * @param node The node we have disconnected from
     * 设置当前与node节点的链接为连接失效
     */
    public void disconnected(int node) {
        nodeState(node).state = ConnectionState.DISCONNECTED;
    }

    /**
     * Get the state of our connection to the given state
     * @param node The id of the node
     * @return The state of our connection
     * 获取与该node的链接状态对象
     */
    private NodeConnectionState nodeState(int node) {
        NodeConnectionState state = this.nodeState.get(node);
        if (state == null)
            throw new IllegalStateException("No entry found for node " + node);
        return state;
    }
}