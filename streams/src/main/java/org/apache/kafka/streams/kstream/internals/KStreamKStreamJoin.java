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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.JoinSideAndKey;
import org.apache.kafka.streams.state.JoinedValues;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

class KStreamKStreamJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamJoin.class);

    private final String otherWindowName;
    private final long joinBeforeMs;
    private final long joinAfterMs;

    private final ValueJoiner<? super V1, ? super V2, ? extends R> joiner;
    private final boolean outer;
    private final Optional<String> outerWindowName;
    private final AtomicLong outerStartWindowTime;
    private final AtomicLong maxObservedStreamTime;
    private final boolean thisJoin;

    KStreamKStreamJoin(final boolean thisJoin,
                       final String otherWindowName,
                       final long joinBeforeMs,
                       final long joinAfterMs,
                       final ValueJoiner<? super V1, ? super V2, ? extends R> joiner,
                       final boolean outer,
                       final Optional<String> outerWindowName,
                       final AtomicLong outerStartWindowTime,
                       final AtomicLong maxObservedStreamTime) {
        this.thisJoin = thisJoin;
        this.otherWindowName = otherWindowName;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.joiner = joiner;
        this.outer = outer;
        this.outerWindowName = outerWindowName;
        this.outerStartWindowTime = outerStartWindowTime;
        this.maxObservedStreamTime = maxObservedStreamTime;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKStreamJoinProcessor();
    }

    private class KStreamKStreamJoinProcessor extends AbstractProcessor<K, V1> {

        private WindowStore<K, V2> otherWindow;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;
        private Optional<WindowStore<JoinSideAndKey<K>, JoinedValues>> outerWindowStore = Optional.empty();

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            otherWindow = context.getStateStore(otherWindowName);

            outerWindowName.ifPresent(name -> {
                outerWindowStore = Optional.of(context.getStateStore(name));
            });
        }


        @SuppressWarnings("unchecked")
        @Override
        public void process(final K key, final V1 value) {
            maxObservedStreamTime.set(Math.max(maxObservedStreamTime.get(), context().timestamp()));

            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (key == null || value == null) {
                LOG.warn(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                droppedRecordsSensor.record();
                return;
            }

            boolean needOuterJoin = outer;

            final long inputRecordTimestamp = context().timestamp();
            final long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
            final long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);

            try (final WindowStoreIterator<V2> iter = otherWindow.fetch(key, timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                    final KeyValue<Long, V2> otherRecord = iter.next();
                    context().forward(
                        key,
                        joiner.apply(value, otherRecord.value),
                        To.all().withTimestamp(Math.max(inputRecordTimestamp, otherRecord.key)));

                    // remove the non-joined record
                    if (timeTo >= maxObservedStreamTime.get()) {
                        outerWindowStore.ifPresent(buffer -> {
                            // this check increases performance around 20% (could it be using bloom filters to quickly check if it exists?)
                            // could RocksDB::keyMayExist() be faster? This check is potentially lighter-weight than invoking DB::Get().
                            if (buffer.fetch(JoinSideAndKey.make(!thisJoin, key), otherRecord.key) != null) {
                                buffer.put(JoinSideAndKey.make(!thisJoin, key), null, otherRecord.key);
                            }
                        });
                    }
                }

                if (needOuterJoin) {
                    // if record is out-of-order, then process it without holding it temporary
                    if (timeTo < maxObservedStreamTime.get()) {
                        context().forward(key, joiner.apply(value, null));
                    } else {
                        // if max stream time in left-topic is higher than right-topic, then the right-topic
                        // max stream time could have expired causing this put() to skip the record
                        outerWindowStore.get().put(
                            JoinSideAndKey.make(thisJoin, key),
                            JoinedValues.make(thisJoin ? value : null, !thisJoin ? value : null),
                            inputRecordTimestamp);
                    }
                }
            }

            // it will also emit expiry records from the other window store if it uses
            // a join (i.e. left join only)
            if (inputRecordTimestamp == maxObservedStreamTime.get()) { // check if input record time move
                maybeEmitOuterExpiryRecords();
            }
        }

        @SuppressWarnings("unchecked")
        private void maybeEmitOuterExpiryRecords() {
            final long expiredWindowTime = maxObservedStreamTime.get() - joinBeforeMs;

            // todo: add grace period to calculations

            // condition to avoid processing records behind the window size
            if (expiredWindowTime >= 0) {
                if (expiredWindowTime > outerStartWindowTime.get()) {
                    outerWindowStore.ifPresent(buffer -> {
                        try (final KeyValueIterator<Windowed<JoinSideAndKey<K>>, JoinedValues> it = buffer.fetchAll(outerStartWindowTime.get(), expiredWindowTime)) {
                            while (it.hasNext()) {
                                final KeyValue<Windowed<JoinSideAndKey<K>>, JoinedValues> e = it.next();
                                if (thisJoin) {
                                    if (e.key.key().isThisJoin()) {
                                        context().forward(e.key.key().getKey(), joiner.apply((V1) e.value.getThisValue(), null));
                                    } else {
                                        context().forward(e.key.key().getKey(), joiner.apply(null, (V2) e.value.getOtherValue()));
                                    }
                                } else {
                                    if (e.key.key().isThisJoin()) {
                                        context().forward(e.key.key().getKey(), joiner.apply(null, (V2) e.value.getThisValue()));
                                    } else {
                                        context().forward(e.key.key().getKey(), joiner.apply((V1) e.value.getOtherValue(), null));
                                    }
                                }

                                buffer.put(e.key.key(), null, e.key.window().start());
                            }
                        }

                        // is this affected by the other join?
                        outerStartWindowTime.set(expiredWindowTime + 1);
                    });
                }
            }
        }
    }
}
