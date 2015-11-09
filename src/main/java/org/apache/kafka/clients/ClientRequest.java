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

import org.apache.kafka.common.requests.RequestSend;

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 * 客户端发送一个请求的对象
 */
public final class ClientRequest {

    private final long createdMs;//请求被创建的时间戳
    private final boolean expectResponse;//true表示期望response在处理完request之后发送一个message返回来
    private final RequestSend request;//发送的request信息
    private final Object attachment;//关联该request的一个对象,该对象随着request一起发送出去

    /**
     * @param createdMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param request The request
     * @param attachment Associated data with the request
     */
    public ClientRequest(long createdMs, boolean expectResponse, RequestSend request, Object attachment) {
        this.createdMs = createdMs;
        this.attachment = attachment;
        this.request = request;
        this.expectResponse = expectResponse;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse + ", payload=" + attachment + ", request=" + request + ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public RequestSend request() {
        return request;
    }

    public Object attachment() {
        return attachment;
    }

    public long createdTime() {
        return createdMs;
    }

}