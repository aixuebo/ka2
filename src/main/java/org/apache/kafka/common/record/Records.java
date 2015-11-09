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
package org.apache.kafka.common.record;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * A binary format which consists of a 4 byte size, an 8 byte offset, and the record bytes. See {@link MemoryRecords}
 * for the in-memory representation.
 */
public interface Records extends Iterable<LogEntry> {

    int SIZE_LENGTH = 4;//记录4个字节的int值,表示该Record所占用的字节大小
    int OFFSET_LENGTH = 8;//记录8个字节的long值,表示该Record的offset偏移量
    int LOG_OVERHEAD = SIZE_LENGTH + OFFSET_LENGTH;

    /**
     * Write these records to the given channel
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails.
     */
    public int writeTo(GatheringByteChannel channel) throws IOException;

    /**
     * The size of these records in bytes
     */
    public int sizeInBytes();

}
