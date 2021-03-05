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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.state.KeyAndJoinSide;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

public class KeyAndJoinSideDeserializer<K> implements WrappingNullableDeserializer<KeyAndJoinSide<K>, K, Void> {
    public final Deserializer<K> keyDeserializer;

    public KeyAndJoinSideDeserializer(final Deserializer<K> keyDeserializer) {
        Objects.requireNonNull(keyDeserializer);
        this.keyDeserializer = keyDeserializer;
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        keyDeserializer.configure(configs, isKey);
    }

    @Override
    public KeyAndJoinSide<K> deserialize(final String topic, final byte[] joinSideAndKey) {
        if (joinSideAndKey == null || joinSideAndKey.length == 0) {
            return null;
        }

        final boolean thisJoin = joinSideAndKey[0] == 1 ? true : false;
        final K key = keyDeserializer.deserialize(topic, rawKey(joinSideAndKey));
        return KeyAndJoinSide.make(thisJoin, key);
    }

    static byte[] rawKey(final byte[] joinSideAndKey) {
        final int rawValueLength = joinSideAndKey.length - 1;

        return ByteBuffer
            .allocate(rawValueLength)
            .put(joinSideAndKey, 1, rawValueLength)
            .array();
    }

    @Override
    public void close() {
        keyDeserializer.close();
    }

    @Override
    public void setIfUnset(final Deserializer<K> defaultKeyDeserializer, final Deserializer<Void> defaultValueDeserializer) {
        // ValueAndTimestampDeserializer never wraps a null deserializer (or configure would throw),
        // but it may wrap a deserializer that itself wraps a null deserializer.
        initNullableDeserializer(keyDeserializer, defaultKeyDeserializer, defaultValueDeserializer);
    }
}

