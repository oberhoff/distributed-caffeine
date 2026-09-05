/*
 * Copyright © 2023-2026 Dr. Andreas Oberhoff (All rights reserved)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.oberhoff.distributedcaffeine;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalKey<K> {

    private final K key;
    private @Nullable String hash;

    private InternalKey(K key) {
        this.key = requireNonNull(key);
    }

    private K getKey() {
        return key;
    }

    @Nullable String getHash() {
        return hash;
    }

    InternalKey<K> setHash(@Nullable String hash) {
        this.hash = hash;
        return this;
    }

    @Override
    public boolean equals(@Nullable Object object) {
        return object instanceof InternalKey<?> that && Objects.equals(this.key, that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return key.toString();
    }

    static <K> InternalKey<K> ik(K key) {
        return new InternalKey<>(key);
    }

    static <K> K k(InternalKey<K> key) {
        return key.getKey();
    }

    static <K> @Nullable K kn(@Nullable InternalKey<K> key) {
        return nonNull(key)
                ? key.getKey()
                : null;
    }
}
