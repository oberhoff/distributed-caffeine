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

import io.github.oberhoff.distributedcaffeine.hasher.Hash;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import org.jspecify.annotations.Nullable;
import org.springframework.cache.interceptor.SimpleKey;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Hashes the keys of Spring's default key generator: a single argument as it is, several or none as a
 * {@link SimpleKey}. A key is hashed in this order:
 * <ol>
 *     <li>implementing {@link Hashable}: by its own hash;</li>
 *     <li>a {@code SimpleKey}: by its params, a custom hash (see below);</li>
 *     <li>of a built-in kind ({@link Hasher#builtInHash}: a {@link String}, {@link Long}, {@link Integer},
 *     {@link UUID} or {@link Enum enum} constant): as the cache hashes it without a hash provider;</li>
 *     <li>anything else: by the fallback, if one is given, else refused.</li>
 * </ol>
 * A {@code SimpleKey}'s hash is built with a {@link Hasher}, as any provider's:
 * {@code "distributed-caffeine:SimpleKey"} ({@link Hasher#putString}, so that no other provider's input begins
 * alike), the number of params, then per param a tag and its data: {@code 0} for {@code null}; {@code 1} and its
 * built-in hash ({@link Hasher#builtInHash}); {@code 2} and a {@code Hashable}'s or the fallback's hash; {@code 3}
 * and a nested {@code SimpleKey}'s hash, each hash put by {@link Hasher#putHash}. The tag keeps params of different
 * kinds apart. Built-in keys keep the hash the cache gives them without the provider; the format is fixed, pinned by
 * SimpleKeyHashProviderTest.
 * <p>
 * {@link DistributedCaffeineCacheManager} sets one on every builder, with the builder's own hash provider, if any, as
 * the fallback.
 *
 * @author Andreas Oberhoff
 */
final class SimpleKeyHashProvider implements HashProvider<Object> {

    // SimpleKey offers no access to its params: read by reflection, its field name pinned by SimpleKeyHashProviderTest.
    // The handle is looked up once, static so that the JIT treats it as a constant on the hit path; Spring on the
    // classpath, or as an automatic module, is open to it
    private static final String PARAMS_FIELD = "params";
    private static final VarHandle PARAMS;

    static {
        try {
            PARAMS = MethodHandles.privateLookupIn(SimpleKey.class, MethodHandles.lookup())
                    .findVarHandle(SimpleKey.class, PARAMS_FIELD, Object[].class);
        } catch (ReflectiveOperationException | SecurityException e) {
            throw new IllegalStateException("Spring's SimpleKey cannot be read by this Spring version: its field "
                    + PARAMS_FIELD + " was not found or is not accessible", e);
        }
    }

    // a SimpleKey's params, also for its serializer (SimpleKeyForySerializer)
    static @Nullable Object[] params(SimpleKey key) {
        return (@Nullable Object[]) PARAMS.get(key);
    }

    // a SimpleKey's hash begins so, and tags each param by its kind (the class's comment)
    private static final String DOMAIN = "distributed-caffeine:SimpleKey";
    private static final byte NULL_PARAM = 0;
    private static final byte BUILT_IN_PARAM = 1;
    private static final byte CUSTOM_PARAM = 2;
    private static final byte SIMPLE_KEY_PARAM = 3;

    private final @Nullable HashProvider<Object> fallback;

    // refuses keys, and params, of other types
    SimpleKeyHashProvider() {
        this.fallback = null;
    }

    // hashes keys, and params, of other types with the fallback
    SimpleKeyHashProvider(HashProvider<Object> fallback) {
        this.fallback = requireNonNull(fallback, "fallback cannot be null");
    }

    @Override
    public Hash getHash(Object key, Supplier<Hasher> hasher) {
        if (key instanceof Hashable hashable) {
            return hashable.getHash(hasher);
        }
        if (key instanceof SimpleKey simpleKey) {
            return hashSimpleKey(simpleKey, hasher);
        }
        Hash builtIn = Hasher.builtInHash(key);
        if (builtIn != null) {
            return builtIn;
        }
        if (fallback != null) {
            return fallback.getHash(key, hasher);
        }
        throw refused(key);
    }

    private Hash hashSimpleKey(SimpleKey key, Supplier<Hasher> hasher) {
        @Nullable Object[] params = params(key);
        Hasher combined = hasher.get().putString(DOMAIN).putInt(params.length);
        for (Object param : params) {
            putParam(param, combined, hasher);
        }
        return combined.getHash();
    }

    private void putParam(@Nullable Object param, Hasher combined, Supplier<Hasher> hasher) {
        if (param == null) {
            combined.putByte(NULL_PARAM);
        } else if (param instanceof Hashable hashable) {
            combined.putByte(CUSTOM_PARAM)
                    .putHash(requireParamHash(hashable.getHash(hasher), "The param's Hashable", param));
        } else if (param instanceof SimpleKey simpleKey) {
            combined.putByte(SIMPLE_KEY_PARAM).putHash(hashSimpleKey(simpleKey, hasher));
        } else {
            Hash builtIn = Hasher.builtInHash(param);
            if (builtIn != null) {
                combined.putByte(BUILT_IN_PARAM).putHash(builtIn);
            } else if (fallback != null) {
                combined.putByte(CUSTOM_PARAM)
                        .putHash(requireParamHash(fallback.getHash(param, hasher), "The hash provider", param));
            } else {
                throw refused(param);
            }
        }
    }

    private static IllegalStateException refused(Object key) {
        return new IllegalStateException(("Keys of type %s cannot be hashed out of the box (only String, Long, "
                + "Integer, UUID, enums and a SimpleKey containing these can): implement Hashable in the key class, "
                + "give the method a key of these types (@Cacheable(key = ...)), or set a hash provider for it on the "
                + "builder").formatted(key.getClass().getName()));
    }

    private static Hash requireParamHash(@Nullable Hash hash, String source, Object param) {
        if (hash == null) {
            throw new IllegalStateException(source + " returned no hash for the SimpleKey param " + param);
        }
        return hash;
    }
}
