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
package io.github.oberhoff.distributedcaffeine.integration.spring;

import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import org.apache.fory.config.Config;
import org.apache.fory.context.ReadContext;
import org.apache.fory.context.WriteContext;
import org.apache.fory.serializer.Serializer;
import org.springframework.cache.interceptor.SimpleKey;

/**
 * The key serializer {@link DistributedCaffeineCacheManager} sets where a builder has none: the default
 * {@link ForySerializer}, but a {@link SimpleKey}, a subclass included, serialized by its params. Fory would
 * serialize a {@code SimpleKey} through its Java serialization path (it customizes Java serialization), logging a
 * warning that it does; by its params, it needs none. Every other type, the params themselves included, is
 * serialized as by the default {@code ForySerializer}. Keys are serialized for diagnostics only; a {@code SimpleKey}
 * read back is {@code new SimpleKey(params)}, equal to the one written (a subclass's own fields aside).
 *
 * @author Andreas Oberhoff
 */
final class SimpleKeyForySerializer extends ForySerializer<Object> {

    SimpleKeyForySerializer() {
        // asked for every class Fory meets, at the top or nested: a SimpleKey of any class by its params, every other
        // class by Fory's default (null)
        fory().registerSerializerFactory((resolver, type) -> SimpleKey.class.isAssignableFrom(type)
                ? new ParamsSerializer(resolver.getConfig(), type.asSubclass(SimpleKey.class)) : null);
    }

    // a SimpleKey as its params, an Object[] written as Fory writes any, so a nested SimpleKey by this serializer
    // again. It is read back as a SimpleKey, whatever its class was
    private static final class ParamsSerializer extends Serializer<SimpleKey> {

        @SuppressWarnings("unchecked") // a SimpleKey of the given class is written, a SimpleKey is read
        ParamsSerializer(Config config, Class<? extends SimpleKey> type) {
            super(config, (Class<SimpleKey>) type);
        }

        @Override
        public void write(WriteContext context, SimpleKey key) {
            context.writeRef(SimpleKeyHashProvider.params(key));
        }

        @Override
        public SimpleKey read(ReadContext context) {
            return new SimpleKey((Object[]) context.readRef());
        }
    }
}
