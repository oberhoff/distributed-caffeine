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

import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import org.jspecify.annotations.Nullable;
import org.springframework.cache.interceptor.SimpleKey;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Hashes the keys of Spring's default key generator: a single argument as it is, several or none as a
 * {@link SimpleKey}. A key, and each param of a {@code SimpleKey}, is hashed in this order:
 * <ol>
 *     <li>implementing {@link Hashable}: by its own hash;</li>
 *     <li>a built-in type ({@link Hasher#putBuiltIn}: a {@code String}, {@code Long}, {@code Integer}, {@code UUID} or
 *     an enum): as the cache hashes it without a hash provider;</li>
 *     <li>a {@code SimpleKey}: by the number of its params and each param's hash ({@code null} included);</li>
 *     <li>anything else: by the fallback, if one is given, else refused.</li>
 * </ol>
 * The first two keep every key the cache hashes itself at the same hash, so the provider changes no stored hash.
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

    // a param that is null: the hash of a param is never empty, so this marker cannot be one
    private static final String NULL_PARAM = "";

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
    public String getHash(Object key, Supplier<Hasher> hasher) {
        if (key instanceof Hashable hashable) {
            return hashable.getHash(hasher);
        }
        if (key instanceof SimpleKey simpleKey) {
            return hashParams((@Nullable Object[]) PARAMS.get(simpleKey), hasher);
        }
        Hasher builtIn = hasher.get();
        if (Hasher.putBuiltIn(key, builtIn)) {
            return builtIn.getHash();
        }
        if (fallback != null) {
            return fallback.getHash(key, hasher);
        }
        throw new IllegalStateException(("Keys of type %s cannot be hashed out of the box (only String, Long, "
                + "Integer, UUID, enums and a SimpleKey containing these can): implement Hashable in the key class, "
                + "give the method a key of these types (@Cacheable(key = ...)), or set a hash provider for it on the "
                + "builder")
                .formatted(key.getClass().getName()));
    }

    // the number of params, then each param's hash: unambiguous without encodings of its own, since a param's hash is
    // the one it has as a key alone
    private String hashParams(@Nullable Object[] params, Supplier<Hasher> hasher) {
        Hasher combined = hasher.get().putInt(params.length);
        for (Object param : params) {
            combined.putString(param == null ? NULL_PARAM : requireParamHash(getHash(param, hasher), param));
        }
        return combined.getHash();
    }

    private static String requireParamHash(@Nullable String hash, Object param) {
        if (hash == null || hash.isEmpty()) {
            throw new IllegalStateException("The hash of the SimpleKey param " + param + " cannot be null or empty");
        }
        return hash;
    }
}
