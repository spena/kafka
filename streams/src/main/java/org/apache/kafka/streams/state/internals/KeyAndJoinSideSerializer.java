/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.state.KeyAndJoinSide;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

public class KeyAndJoinSideSerializer<K> implements WrappingNullableSerializer<KeyAndJoinSide<K>, K, Void> {
    public final Serializer<K> keySerializer;

    KeyAndJoinSideSerializer(final Serializer<K> keySerializer) {
        Objects.requireNonNull(keySerializer);
        this.keySerializer = keySerializer;
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        keySerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic,
                            final KeyAndJoinSide<K> data) {
        if (data == null) {
            return null;
        }
        return serialize(topic, data.isThisJoin(), data.getKey());
    }

    public byte[] serialize(final String topic,
                            final boolean thisJoin,
                            final K key) {
        final byte[] rawKey = keySerializer.serialize(topic, key);

        // Since we can't control the result of the internal serializer, we make sure that the result
        // is not null as well.
        // Serializing non-null values to null can be useful when working with Optional-like values
        // where the Optional.empty case is serialized to null.
        // See the discussion here: https://github.com/apache/kafka/pull/7679
        //if (rawValue == null) {
        //    return null;
        //}

        return ByteBuffer
            .allocate(1 + rawKey.length)
            .put((byte) (thisJoin ? 1 : 0))
            .put(rawKey)
            .array();
    }

    @Override
    public void close() {
        keySerializer.close();
    }

    @Override
    public void setIfUnset(final Serializer<K> defaultKeySerializer, final Serializer<Void> defaultValueSerializer) {
        // ValueAndTimestampSerializer never wraps a null serializer (or configure would throw),
        // but it may wrap a serializer that itself wraps a null serializer.
        initNullableSerializer(keySerializer, defaultKeySerializer, defaultValueSerializer);
    }
}
