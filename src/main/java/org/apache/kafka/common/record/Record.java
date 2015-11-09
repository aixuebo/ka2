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

import java.nio.ByteBuffer;

import org.apache.kafka.common.utils.Crc32;
import org.apache.kafka.common.utils.Utils;


/**
 * A record: a serialized key and value along with the associated CRC and other fields
 * 记录一个key-value的字节数组,以及压缩方式
 * 注意:
 * 1.key可以是null
 * 2.value不允许是null
 * 3.压缩方式默认是0,即不支持压缩
 */
public final class Record {

    /**
     * The current offset and size for all the fixed-length fields
     * 格式:4个字节crc、1个字节MAGIC、1个字节ATTRIBUTE、4个字节KEY_SIZE_LENGTH、4个字节VALUE_SIZE_LENGTH
     */
    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTE_LENGTH = 1;
    public static final int KEY_SIZE_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET = KEY_SIZE_OFFSET + KEY_SIZE_LENGTH;
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * The size for the record header
     * 头文件占用6个字节,分别是 4个字节crc、1个字节MAGIC、1个字节ATTRIBUTE
     */
    public static final int HEADER_SIZE = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH;

    /**
     * The amount of overhead bytes in a record
     * 除了key-value的字节数组外,还要多加14个字节,分别是:4个字节crc、1个字节MAGIC、1个字节ATTRIBUTE、4个字节KEY_SIZE_LENGTH、4个字节VALUE_SIZE_LENGTH
     */
    public static final int RECORD_OVERHEAD = HEADER_SIZE + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * The current "magic" value
     * 当前magic所占用的一个字节内容是0
     */
    public static final byte CURRENT_MAGIC_VALUE = 0;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec. 0 is reserved to indicate no
     * compression
     */
    public static final int COMPRESSION_CODEC_MASK = 0x07;

    /**
     * Compression code for uncompressed records
     */
    public static final int NO_COMPRESSION = 0;

    private final ByteBuffer buffer;

    public Record(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * A constructor to create a LogRecord. If the record's compression type is not none, then
     * its value payload should be already compressed with the specified type; the constructor
     * would always write the value payload as is and will not do the compression itself.
     * 
     * @param key The key of the record (null, if none)
     * @param value The record value
     * @param type The compression type used on the contents of the record (if any)
     * @param valueOffset The offset into the payload array used to extract payload
     * @param valueSize The size of the payload to use
     * 
     * valueOffset和valueSize二选一，作为value值的获取方式
     * 1.valueSize>=0,说明从valueOffset开始,要从value中获取valueSize个字节,相当于write(value,valueOffset,valueSize)
     * 2.valueSize<0,说明从valueOffset开始到value的最后都是该value信息,相当于write(value,valueOffset,value.length-valueOffset)
     */
    public Record(byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        this(ByteBuffer.allocate(recordSize(key == null ? 0 : key.length,
            value == null ? 0 : valueSize >= 0 ? valueSize : value.length - valueOffset)));
        write(this.buffer, key, value, type, valueOffset, valueSize);
        this.buffer.rewind();//重新将position位置设置成0,即相当于重新开始写入buffer
    }

    public Record(byte[] key, byte[] value, CompressionType type) {
        this(key, value, type, 0, -1);
    }

    public Record(byte[] value, CompressionType type) {
        this(null, value, type);
    }

    public Record(byte[] key, byte[] value) {
        this(key, value, CompressionType.NONE);
    }

    //默认是不压缩,可以可以设置为null
    public Record(byte[] value) {
        this(null, value, CompressionType.NONE);
    }

    // Write a record to the buffer, if the record's compression type is none, then
    // its value payload should be already compressed with the specified type
    public static void write(ByteBuffer buffer, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        // construct the compressor with compression type none since this function will not do any
        //compression according to the input type, it will just write the record's payload as is
        Compressor compressor = new Compressor(buffer, CompressionType.NONE, buffer.capacity());
        compressor.putRecord(key, value, type, valueOffset, valueSize);
    }

    /**
     * @param compressor
     * @param crc
     * @param attributes
     * @param key
     * @param value
     * @param valueOffset
     * @param valueSize
     * valueOffset和valueSize二选一，作为value值的获取方式
     * 1.valueSize>=0,说明从valueOffset开始,要从value中获取valueSize个字节,相当于write(value,valueOffset,valueSize)
     * 2.valueSize<0,说明从valueOffset开始到value的最后都是该value信息,相当于write(value,valueOffset,value.length-valueOffset)
     */
    public static void write(Compressor compressor, long crc, byte attributes, byte[] key, byte[] value, int valueOffset, int valueSize) {
        // write crc
        compressor.putInt((int) (crc & 0xffffffffL));
        // write magic value
        compressor.putByte(CURRENT_MAGIC_VALUE);
        // write attributes
        compressor.putByte(attributes);
        // write the key
        if (key == null) {
            compressor.putInt(-1);
        } else {
            compressor.putInt(key.length);
            compressor.put(key, 0, key.length);
        }
        // write the value
        if (value == null) {
            compressor.putInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            compressor.putInt(size);
            compressor.put(value, valueOffset, size);
        }
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    /**
     * @param keySize key所占用的字节数组长度
     * @param valueSize value所占用的字节数组长度
     * @return 要存储该key-value需要的总字节数
     * 4个字节crc、1个字节MAGIC、1个字节ATTRIBUTE、4个字节KEY_SIZE_LENGTH、4个字节VALUE_SIZE_LENGTH
     */
    public static int recordSize(int keySize, int valueSize) {
        return CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH + KEY_SIZE_LENGTH + keySize + VALUE_SIZE_LENGTH + valueSize;
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    //根据压缩类型的mask,计算attributes
    public static byte computeAttributes(CompressionType type) {
        byte attributes = 0;
        if (type.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        return attributes;
    }

    /**
     * Compute the checksum of the record from the record contents
     * 计算校验和
     */
    public static long computeChecksum(ByteBuffer buffer, int position, int size) {
        Crc32 crc = new Crc32();
        crc.update(buffer.array(), buffer.arrayOffset() + position, size);
        return crc.getValue();
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     * 计算校验和
     */
    public static long computeChecksum(byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        Crc32 crc = new Crc32();
        crc.update(CURRENT_MAGIC_VALUE);
        byte attributes = 0;
        if (type.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        crc.update(attributes);
        // update for the key
        if (key == null) {
            crc.updateInt(-1);
        } else {
            crc.updateInt(key.length);
            crc.update(key, 0, key.length);
        }
        // update for the value
        if (value == null) {
            crc.updateInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            crc.updateInt(size);
            crc.update(value, valueOffset, size);
        }
        return crc.getValue();
    }


    /**
     * Compute the checksum of the record from the record contents
     * 计算抛出crc4个字节位置后的数据,进行校验和运算
     */
    public long computeChecksum() {
        return computeChecksum(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * Retrieve the previously computed CRC for this record
     * 读取校验和
     */
    public long checksum() {
        return Utils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * Returns true if the crc stored with the record matches the crc computed off the record contents
     * 计算校验和是否相同
     */
    public boolean isValid() {
        return checksum() == computeChecksum();
    }

    /**
     * Throw an InvalidRecordException if isValid is false for this record
     * 确保校验和是正确的
     */
    public void ensureValid() {
        if (!isValid())
            throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum()
                                             + ", computed crc = "
                                             + computeChecksum()
                                             + ")");
    }

    /**
     * The complete serialized size of this record in bytes (including crc, header attributes, etc)
     * buffer中的数据存储的字节数
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     * key的字节数
     */
    public int keySize() {
        return buffer.getInt(KEY_SIZE_OFFSET);
    }

    /**
     * Does the record have a key?
     * 是否存在key的字节,即可以是否为null,如果为null,则key的位置写入的值是-1
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the value size is stored
     * 获取buffer中存储value的字节数的数据位置,即key的数据位置+key的数据大小
     */
    private int valueSizeOffset() {
        return KEY_OFFSET + Math.max(0, keySize());
    }

    /**
     * The length of the value in bytes
     * 获取value对应的字节数
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * The magic version of this record
     * 获取magic所对应的一个字节
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * The attributes stored with this record
     * 获取attribute所对应的一个字节
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * The compression type used with this record、
     * 获取压缩方式
     */
    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * A ByteBuffer containing the value of this record
     * 获取value对应的ByteBuffer
     * 参数是value对应的字节数的位置
     */
    public ByteBuffer value() {
        return sliceDelimited(valueSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     *  获取key对应的ByteBuffer
     *  参数是key对应的字节数的位置
     */
    public ByteBuffer key() {
        return sliceDelimited(KEY_SIZE_OFFSET);
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    public String toString() {
        return String.format("Record(magic = %d, attributes = %d, crc = %d, key = %d bytes, value = %d bytes)",
                             magic(),
                             attributes(),
                             checksum(),
                             key().limit(),
                             value().limit());
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (!other.getClass().equals(Record.class))
            return false;
        Record record = (Record) other;
        return this.buffer.equals(record.buffer);
    }

    public int hashCode() {
        return buffer.hashCode();
    }

}
