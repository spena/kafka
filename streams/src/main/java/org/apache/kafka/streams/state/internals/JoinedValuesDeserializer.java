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
import org.apache.kafka.streams.state.JoinedValues;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

public class JoinedValuesDeserializer<V1, V2> implements WrappingNullableDeserializer<JoinedValues<V1, V2>, Void, JoinedValues<V1, V2>> {
    public final Deserializer<V1> thisDeserializer;
    public final Deserializer<V2> otherDeserializer;

    public JoinedValuesDeserializer(final Deserializer<V1> thisDeserializer, final Deserializer<V2> otherDeserializer) {
        this.thisDeserializer = Objects.requireNonNull(thisDeserializer);
        this.otherDeserializer = Objects.requireNonNull(otherDeserializer);
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        thisDeserializer.configure(configs, isKey);
        otherDeserializer.configure(configs, isKey);
    }

    @Override
    public JoinedValues<V1, V2> deserialize(final String topic, final byte[] joinedValues) {
        if (joinedValues == null || joinedValues.length == 0) {
            return null;
        }

        final boolean thisJoin = joinedValues[0] == 1 ? true : false;
        final V1 thisValue = thisJoin ? thisDeserializer.deserialize(topic, rawValue(joinedValues)) : null;
        final V2 otherValue = !thisJoin ? otherDeserializer.deserialize(topic, rawValue(joinedValues)) : null;
        return JoinedValues.make(thisValue, otherValue);
    }

    static byte[] rawValue(final byte[] joinedValues) {
        final int rawValueLength = joinedValues.length - 1;

        return ByteBuffer
            .allocate(rawValueLength)
            .put(joinedValues, 1, rawValueLength)
            .array();
    }

    @Override
    public void setIfUnset(final Deserializer<Void> defaultKeyDeserializer, final Deserializer<JoinedValues<V1, V2>> defaultValueDeserializer) {
        // ValueAndTimestampDeserializer never wraps a null deserializer (or configure would throw),
        // but it may wrap a deserializer that itself wraps a null deserializer.
        initNullableDeserializer(thisDeserializer, defaultKeyDeserializer, defaultValueDeserializer);
        initNullableDeserializer(otherDeserializer, defaultKeyDeserializer, defaultValueDeserializer);
    }
}
