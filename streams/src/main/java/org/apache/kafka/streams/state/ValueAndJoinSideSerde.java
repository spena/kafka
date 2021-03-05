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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerde;
import org.apache.kafka.streams.state.internals.ValueAndJoinSideDeserializer;
import org.apache.kafka.streams.state.internals.ValueAndJoinSideSerializer;

import static java.util.Objects.requireNonNull;

public class ValueAndJoinSideSerde<V1, V2> extends WrappingNullableSerde<ValueAndJoinSide<V1, V2>, Void, ValueAndJoinSide<V1, V2>> {
    public ValueAndJoinSideSerde(final Serde<V1> thisValueSerde, final Serde<V2> otherValueSerde) {
        super(
            new ValueAndJoinSideSerializer<>(
                requireNonNull(thisValueSerde, "thisValueSerde was null").serializer(),
                requireNonNull(otherValueSerde, "thisValueSerde was null").serializer()),
            new ValueAndJoinSideDeserializer<>(
                requireNonNull(thisValueSerde, "otherValueSerde was null").deserializer(),
                requireNonNull(otherValueSerde, "otherValueSerde was null").deserializer())
        );
    }
}

