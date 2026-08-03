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

// implemented by everything that is wired from the instance registry instead of through its own constructor. That
// detour is unavoidable for the parts taking part in building the very Caffeine cache they depend on: the listeners
// and the cache loader are handed to the builder, while the cache manager needs the cache that comes out of it
interface InternalInitializable<K, V> {

    // invoked exactly once per instance, see InternalInstanceRegistry.initializeComponents()
    void initialize(InternalInstanceRegistry<K, V> instanceRegistry);
}
