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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.nio.ByteBuffer;
import java.util.List;

public class TimestampedOrderedKeySchema implements RocksDBSegmentedBytesStore.KeySchema {
    private static final int SEQNUM_SIZE = 4;
    private static final int TIMESTAMP_SIZE = 8;

    public static Bytes lowerTimestampRange(final long from) {
        final ByteBuffer rangeStart = ByteBuffer.allocate(TIMESTAMP_SIZE);

        return Bytes.wrap(
            rangeStart
                .putLong(from)
                .array()
        );
    }

    public static Bytes upperTimestampRange(final long to) {
        final ByteBuffer rangeStart = ByteBuffer.allocate(TIMESTAMP_SIZE);

        return Bytes.wrap(
            rangeStart
                .putLong(to)
                .array()
        );
    }

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        final byte[] maxSuffix = ByteBuffer.allocate(SEQNUM_SIZE)
            .putInt(Integer.MAX_VALUE)
            .array();

        return OrderedBytes.upperRangeTimestamp(to, key, maxSuffix);
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        return OrderedBytes.lowerRangeTimestamp(from, key, new byte[SEQNUM_SIZE]);
    }

    @Override
    public Bytes upperRangeFixedSize(final Bytes key, final long to) {
        return TimestampedOrderedKeySchema.toStoreKeyBinary(key, to, Integer.MAX_VALUE);
    }

    @Override
    public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
        return TimestampedOrderedKeySchema.toStoreKeyBinary(key, Math.max(0, from), 0);
    }

    @Override
    public long segmentTimestamp(final Bytes key) {
        return TimestampedOrderedKeySchema.extractStoreTimestamp(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to) {
        return iterator -> {
            while (iterator.hasNext()) {
                final Bytes bytes = iterator.peekNextKey();
                final Bytes keyBytes = Bytes.wrap(TimestampedOrderedKeySchema.extractStoreKeyBytes(bytes.get()));
                final long time = TimestampedOrderedKeySchema.extractStoreTimestamp(bytes.get());
                if ((binaryKeyFrom == null || keyBytes.compareTo(binaryKeyFrom) >= 0)
                    && (binaryKeyTo == null || keyBytes.compareTo(binaryKeyTo) <= 0)
                    && time >= from
                    && time <= to) {
                    return true;
                }
                iterator.next();
            }
            return false;
        };
    }

    @Override
    public <S extends Segment> List<S> segmentsToSearch(final Segments<S> segments, final long from, final long to, final boolean forward) {
        return segments.segments(from, to, forward);
    }

    /**
     * Safely construct a time window of the given size,
     * taking care of bounding endMs to Long.MAX_VALUE if necessary
     */
    static TimeWindow timeWindowForSize(final long startMs,
                                        final long windowSize) {
        long endMs = startMs + windowSize;

        if (endMs < 0) {
            //LOG.warn("Warning: window end time was truncated to Long.MAX");
            endMs = Long.MAX_VALUE;
        }
        return new TimeWindow(startMs, endMs);
    }

    // for store serdes

    public static Bytes toStoreKeyBinary(final Bytes key,
                                         final long timestamp,
                                         final int seqnum) {
        final byte[] serializedKey = key.get();
        return toStoreKeyBinary(serializedKey, timestamp, seqnum);
    }

    // package private for testing
    static Bytes toStoreKeyBinary(final byte[] serializedKey,
                                  final long timestamp,
                                  final int seqnum) {
        final ByteBuffer buf = ByteBuffer.allocate(TIMESTAMP_SIZE + serializedKey.length + SEQNUM_SIZE);
        buf.putLong(timestamp);
        buf.put(serializedKey);
        buf.putInt(seqnum);

        return Bytes.wrap(buf.array());
    }

    static byte[] extractStoreKeyBytes(final byte[] binaryKey) {
        final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, TIMESTAMP_SIZE, bytes, 0, bytes.length);
        return bytes;
    }

    static long extractStoreTimestamp(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(0);
    }

    public static Windowed<Bytes> fromStoreBytesKey(final byte[] binaryKey,
                                                    final long windowSize) {
        final Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
        final Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    static Window extractStoreWindow(final byte[] binaryKey,
                                     final long windowSize) {
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(0);
        return timeWindowForSize(start, windowSize);
    }
}
