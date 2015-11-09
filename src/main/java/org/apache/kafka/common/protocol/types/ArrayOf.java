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
package org.apache.kafka.common.protocol.types;

import java.nio.ByteBuffer;

/**
 * Represents a type for an array of a particular type
 * 数组,但是数组的元素必须是同一个类型的
 */
public class ArrayOf extends Type {

    private final Type type;//数组的类型

    public ArrayOf(Type type) {
        this.type = type;
    }

    //将数组全部信息都写入到buffer中
    @Override
    public void write(ByteBuffer buffer, Object o) {
        Object[] objs = (Object[]) o;
        int size = objs.length;
        buffer.putInt(size);
        for (int i = 0; i < size; i++)
            type.write(buffer, objs[i]);
    }

    //从ByteBuffer中读取数组,即读取多个元素,组成集合
    @Override
    public Object read(ByteBuffer buffer) {
        int size = buffer.getInt();
        Object[] objs = new Object[size];
        for (int i = 0; i < size; i++)
            objs[i] = type.read(buffer);
        return objs;
    }

    //数组中所有的元素总大小
    @Override
    public int sizeOf(Object o) {
        Object[] objs = (Object[]) o;
        int size = 4;
        for (int i = 0; i < objs.length; i++)
            size += type.sizeOf(objs[i]);
        return size;
    }

    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return "ARRAY(" + type + ")";
    }

    @Override
    public Object[] validate(Object item) {
        try {
            Object[] array = (Object[]) item;
            for (int i = 0; i < array.length; i++)
                type.validate(array[i]);
            return array;
        } catch (ClassCastException e) {
            throw new SchemaException("Not an Object[].");
        }
    }
}
