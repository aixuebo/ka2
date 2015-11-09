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

/**
 * The state of our connection to a node
 * 每一个节点对应一个该对象
 * 表示该客户端与该节点的连接状态与最后连接时间
 */
final class NodeConnectionState {

    ConnectionState state;//连接状态
    long lastConnectAttemptMs;//最后一次连接的时间

    public NodeConnectionState(ConnectionState state, long lastConnectAttempt) {
        this.state = state;
        this.lastConnectAttemptMs = lastConnectAttempt;
    }

    public String toString() {
        return "NodeState(" + state + ", " + lastConnectAttemptMs + ")";
    }
}