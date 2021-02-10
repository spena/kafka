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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class StreamStreamOuterJoinBuffer<K, V> {
    private final WindowStore<K, V> windowStore;

    public StreamStreamOuterJoinBuffer(final WindowStore<K, V> windowStore) {
        this.windowStore = requireNonNull(windowStore, "windowStore");
    }

    void put(final K key, final V value, final long startTimestamp) {
        windowStore.put(key, value, startTimestamp);
    }

    // untilTimestap is inclusive
    // todo: should return an iterator?
    Map<Windowed<K>, V> delete(final K key, final long untilTimestamp) {
        final Map<Windowed<K>, V> orderedHashMap = new LinkedHashMap<>();

        // todo: choose a better timeFrom? 0 because the other store min. is not known
        // todo: is this delete efficient?
        try (final KeyValueIterator<Windowed<K>, V> it = windowStore.fetch(key, key, 0, untilTimestamp)) {
            while (it.hasNext()) {
                final KeyValue<Windowed<K>, V> e = it.next();
                windowStore.put(e.key.key(), null, e.key.window().start());
                orderedHashMap.put(e.key, e.value);
            }
        }

        return orderedHashMap;
    }

    // untilTimestamp is inclusive
    // todo: should return an iterator?
    Map<Windowed<K>, V> deleteAll(final long untilTimestamp) {
        final Map<Windowed<K>, V> orderedHashMap = new LinkedHashMap<>();

        // todo: choose a better timeFrom? 0 because the other store min. is not known
        // todo: is this delete efficient?
        try (final KeyValueIterator<Windowed<K>, V> it = windowStore.fetchAll(0, untilTimestamp)) {
            while (it.hasNext()) {
                final KeyValue<Windowed<K>, V> e = it.next();
                windowStore.put(e.key.key(), null, e.key.window().start());
                orderedHashMap.put(e.key, e.value);
            }
        }

        return orderedHashMap;
    }
}
