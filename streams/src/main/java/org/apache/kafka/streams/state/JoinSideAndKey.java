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

import java.util.Objects;

public class JoinSideAndKey<K> {
    private final K key;
    private final boolean thisJoin;

    private JoinSideAndKey(final boolean thisJoin,
                           final K key) {
        Objects.requireNonNull(key);
        this.key = key;
        this.thisJoin = thisJoin;
    }

    public static <K> JoinSideAndKey<K> make(final boolean thisJoin,
                                                final K key) {
        return new JoinSideAndKey<>(thisJoin, key);
    }

    public boolean isThisJoin() {
        return thisJoin;
    }

    public K getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "<" + thisJoin + "," + key + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JoinSideAndKey<?> that = (JoinSideAndKey<?>) o;
        return thisJoin == that.thisJoin &&
            Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thisJoin, key);
    }
}
