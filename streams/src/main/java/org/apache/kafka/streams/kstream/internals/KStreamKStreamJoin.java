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

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

class KStreamKStreamJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamJoin.class);

    private final String otherWindowName;
    private final long joinBeforeMs;
    private final long joinAfterMs;

    private final ValueJoiner<? super V1, ? super V2, ? extends R> joiner;
    private final boolean outer;
    private final Optional<String> thisOuterWindowName;
    private final Optional<String> otherOuterWindowName;

    KStreamKStreamJoin(final String otherWindowName,
                       final long joinBeforeMs,
                       final long joinAfterMs,
                       final ValueJoiner<? super V1, ? super V2, ? extends R> joiner,
                       final boolean outer,
                       final Optional<String> thisOuterWindowName,
                       final Optional<String> otherOuterWindowName) {
        this.otherWindowName = otherWindowName;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.joiner = joiner;
        this.outer = outer;
        this.thisOuterWindowName = thisOuterWindowName;
        this.otherOuterWindowName = otherOuterWindowName;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKStreamJoinProcessor();
    }

    private class KStreamKStreamJoinProcessor extends AbstractProcessor<K, V1> {

        private WindowStore<K, V2> otherWindow;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;
        private Optional<WindowStore<K, V1>> thisOuterWindowStore = Optional.empty();
        private Optional<WindowStore<K, V2>> otherOuterWindowStore = Optional.empty();
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
        private long outerStoreStartTime = Long.MAX_VALUE;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            otherWindow = (WindowStore<K, V2>) context.getStateStore(otherWindowName);

            thisOuterWindowName.ifPresent(name ->
                thisOuterWindowStore = Optional.of(context.getStateStore(name)));

            otherOuterWindowName.ifPresent(name ->
                otherOuterWindowStore = Optional.of(context.getStateStore(name)));
        }


        @Override
        public void process(final K key, final V1 value) {
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

            observedStreamTime = Math.max(observedStreamTime, context().timestamp());
            outerStoreStartTime = Math.min(outerStoreStartTime, inputRecordTimestamp);

            try (final WindowStoreIterator<V2> iter = otherWindow.fetch(key, timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                    final KeyValue<Long, V2> otherRecord = iter.next();
                    context().forward(
                        key,
                        joiner.apply(value, otherRecord.value),
                        To.all().withTimestamp(Math.max(inputRecordTimestamp, otherRecord.key)));

                    // remove the non-joined record
                    otherOuterWindowStore.ifPresent(store -> deleteToTimestamp(store, key, inputRecordTimestamp));
                }

                if (needOuterJoin) {
                    thisOuterWindowStore.get().put(key, value, inputRecordTimestamp);
                }

                thisOuterWindowStore.ifPresent(store -> maybeEmitThisExpiryRecords());
                otherOuterWindowStore.ifPresent(store -> maybeEmitOtherExpiryRecords());
            }
        }

        private void deleteToTimestamp(final WindowStore<K, V2> windowStore, final K key, final long timestamp) {
            // chose a better timeFrom
            try (final KeyValueIterator<Windowed<K>, V2> it = windowStore.fetch(key, key, 0, timestamp)) {
                while (it.hasNext()) {
                    final KeyValue<Windowed<K>, V2> e = it.next();
                    windowStore.put(e.key.key(), null, e.key.window().start());
                }
            }
        }

        private void maybeEmitThisExpiryRecords() {
            if (outerStoreStartTime + joinAfterMs /* + grace period */ < observedStreamTime) {
                try (final KeyValueIterator<Windowed<K>, V1> it = thisOuterWindowStore.get().fetchAll(outerStoreStartTime, observedStreamTime)) {
                    while (it.hasNext()) {
                        final KeyValue<Windowed<K>, V1> e = it.next();
                        final long expiryTime = e.key.window().end(); // + grace period

                        if (expiryTime < observedStreamTime) {
                            outerStoreStartTime = e.key.window().start();

                            // delete the record
                            thisOuterWindowStore.get().put(e.key.key(), null, e.key.window().start());
                            context().forward(e.key.key(), joiner.apply(e.value, null));
                        } else {
                            // fetchAll() returns an timestamped ordered iterator. If one row is not expired yet, then
                            // we stop here as the rest of the records won't be expired either
                            break;
                        }
                    }
                }
            }
        }

        private void maybeEmitOtherExpiryRecords() {
            if (outerStoreStartTime + joinAfterMs /* + grace period */ < observedStreamTime) {
                try (final KeyValueIterator<Windowed<K>, V2> it = otherOuterWindowStore.get().fetchAll(outerStoreStartTime, observedStreamTime)) {
                    while (it.hasNext()) {
                        final KeyValue<Windowed<K>, V2> e = it.next();
                        final long expiryTime = e.key.window().end(); // + grace period

                        if (expiryTime < observedStreamTime) {
                            outerStoreStartTime = e.key.window().start();

                            // delete the record
                            otherOuterWindowStore.get().put(e.key.key(), null, e.key.window().start());
                            context().forward(e.key.key(), joiner.apply(null, e.value));
                        } else {
                            // fetchAll() returns an timestamped ordered iterator. If one row is not expired yet, then
                            // we stop here as the rest of the records won't be expired either
                            break;
                        }
                    }
                }
            }
        }
    }
}
