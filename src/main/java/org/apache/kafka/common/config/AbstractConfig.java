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
package org.apache.kafka.common.config;

import java.util.*;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenient base class for configurations to extend.
 * <p>
 * This class holds both the original configuration that was provided as well as the parsed
 */
public class AbstractConfig {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /* configs for which values have been requested, used to detect unused configs 表示在value中哪些key已经被使用过*/
    private final Set<String> used;

    /* the original values passed in by the user 所有的key都是String类型的,因为ConfigDef中的name都是String类型的,目的是为了匹配ConfigDef中的String类型的name*/
    //原始参数键值对信息
    private final Map<String, ?> originals;

    /* the parsed values 经过解析后的参数信息,该信息的key仅是ConfigDef存在key的集合*/
    private final Map<String, Object> values;

    @SuppressWarnings("unchecked")
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals) {
        /* check that all the keys are really strings 首先检查originals的key必须都是String类型的,因为ConfigDef中的name都是String类型的,目的是为了匹配ConfigDef中的String类型的name*/
        for (Object key : originals.keySet())
            if (!(key instanceof String))
                throw new ConfigException(key.toString(), originals.get(key), "Key must be a string.");
        
        
        this.originals = (Map<String, ?>) originals;
        this.values = definition.parse(this.originals);
        this.used = Collections.synchronizedSet(new HashSet<String>());
        //打印所有的解析后的value属性值
        logAll();
    }

    //必须从values中获取key,因为value才是真正有意义的key的集合,获取对应的value值
    protected Object get(String key) {
        if (!values.containsKey(key))
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        used.add(key);//key已经被使用过
        return values.get(key);
    }

    public int getInt(String key) {
        return (Integer) get(key);
    }

    public long getLong(String key) {
        return (Long) get(key);
    }

    public double getDouble(String key) {
        return (Double) get(key);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return (List<String>) get(key);
    }

    public boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public Class<?> getClass(String key) {
        return (Class<?>) get(key);
    }

    //获取在原始参数中没有被使用过的key
    public Set<String> unused() {
        Set<String> keys = new HashSet<String>(originals.keySet());
        keys.removeAll(used);
        return keys;
    }

    //clone原始参数集合
    public Map<String, ?> originals() {
        Map<String, Object> copy = new HashMap<String, Object>();
        copy.putAll(originals);
        return copy;
    }

    //打印所有的解析后的value属性值
    private void logAll() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);
        for (Map.Entry<String, Object> entry : this.values.entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }

    /**
     * Log warnings for any unused configurations
     * 打印在原始参数中没有被使用过的key
     */
    public void logUnused() {
        for (String key : unused())
            log.warn("The configuration {} = {} was supplied but isn't a known config.", key, this.values.get(key));
    }

    /**
     * Get a configured instance of the give class specified by the given configuration key. If the object implements
     * Configurable configure it using the configuration.
     * 
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @return A configured instance of the class
     * 获取key对应的class对象,该对象必须是参数t的子类
     */
    public <T> T getConfiguredInstance(String key, Class<T> t) {
        Class<?> c = getClass(key);
        if (c == null)
            return null;
        Object o = Utils.newInstance(c);
        if (!t.isInstance(o))
            throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
        if (o instanceof Configurable)
            ((Configurable) o).configure(this.originals);
        return t.cast(o);
    }

    /**
     * 获取key对应的class集合,该集合是按照key对应的value按照空格拆分而成的
     * 该集合每一个class对象都是参数t的子类,因此实例化每一个class对象,存储到List集合中 
     */
    public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
        List<String> klasses = getList(key);
        List<T> objects = new ArrayList<T>();
        for (String klass : klasses) {
            Class<?> c;
            try {
                c = Class.forName(klass);
            } catch (ClassNotFoundException e) {
                throw new ConfigException(key, klass, "Class " + klass + " could not be found.");
            }
            if (c == null)
                return null;
            Object o = Utils.newInstance(c);
            if (!t.isInstance(o))
                throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
            if (o instanceof Configurable)
                ((Configurable) o).configure(this.originals);
            objects.add(t.cast(o));
        }
        return objects;
    }

}
