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

public class JoinedValues<V1, V2> {
    private final V1 thisValue;
    private final V2 otherValue;

    private JoinedValues(final V1 thisValue,
                         final V2 otherValue) {
        this.thisValue = thisValue;
        this.otherValue = otherValue;
    }

    public static <V1, V2> JoinedValues<V1, V2> make(final V1 thisValue,
                                                     final V2 otherValue) {
        return new JoinedValues<>(thisValue, otherValue);
    }

    public V1 getThisValue() {
        return thisValue;
    }

    public V2 getOtherValue() {
        return otherValue;
    }

    @Override
    public String toString() {
        return "<" + thisValue + "," + otherValue + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JoinedValues<?, ?> that = (JoinedValues<?, ?>) o;
        return thisValue == that.thisValue &&
            Objects.equals(otherValue, that.otherValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thisValue, otherValue);
    }
}
