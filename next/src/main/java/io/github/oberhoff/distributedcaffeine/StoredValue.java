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

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * A value as the store holds it, read directly from the store.
 *
 * @param value the value
 * @param age   how long ago it was written, by the store's clock
 * @param <V>   the value type
 */
public record StoredValue<V>(V value, Duration age) {

    public StoredValue {
        requireNonNull(value, "value cannot be null");
        requireNonNull(age, "age cannot be null");
    }
}
