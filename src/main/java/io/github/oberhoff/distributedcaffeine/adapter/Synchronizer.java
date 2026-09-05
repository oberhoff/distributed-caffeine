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
package io.github.oberhoff.distributedcaffeine.adapter;

/**
 * Interface representing a synchronizer that manages distributed synchronization between cache instances using an
 * underlying store.
 * <p>
 * <b>Note:</b> Inbound changes of the underlying store are supposed to be passed on as cache entries by the adapter
 * using the {@link Receiver}. A cache entry that could not be read for whatever reason (e.g. deserialization fails or
 * field values do not meet the conditions of a cache entry) should be skipped and logged instead of breaking the
 * synchronization exceptionally.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public interface Synchronizer<K, V> extends IdentifierAware, DiscriminatorAware, StateAware, SerializerAware<K, V>,
        ReceiverAware<K, V> {
}
