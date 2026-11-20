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
package io.github.oberhoff.distributedcaffeine.hasher;

import com.dynatrace.hash4j.hashing.HashSink;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Hasher} offers the put methods of hash4j's {@code HashSink}, with types of its own in place of hash4j's, so
 * that no hash4j type is part of the API: a hint when a hash4j upgrade adds or drops one. A method added there is
 * added here with its encoding (and to {@link HasherDifferentialTest}), or listed as left out; a method dropped there
 * stays here, as its encoding is ours, and is listed as kept.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class HasherApiTest {

    // hash4j's types, as Hasher's own
    private static final Map<String, String> TYPES = Map.of("HashFunnel", "Funnel");

    // HashSink's methods Hasher offers otherwise or not: an element's hash by a hasher of its own, which only hash4j's
    // types could name, is putUnorderedIterable(Iterable, Funnel) here, by XXH3-128; bytes read through a ByteAccess
    // (a ByteBuffer's, a memory segment's) are left out, a type of their own for one method
    private static final Set<String> LEFT_OUT = Set.of(
            "putUnorderedIterable(Iterable,Funnel,HashStream64)",
            "putUnorderedIterable(Iterable,Funnel,Hasher64)",
            "putBytes(Object,long,long,ByteAccess)");

    // Hasher's put methods HashSink has not: the replacement above, and a Hash's bytes
    private static final Set<String> OWN = Set.of("putUnorderedIterable(Iterable,Funnel)", "putHash(Hash)");

    private static String signature(Method method) {
        return method.getName() + Arrays.stream(method.getParameterTypes())
                .map(type -> TYPES.getOrDefault(type.getSimpleName(), type.getSimpleName()))
                .collect(Collectors.joining(",", "(", ")"));
    }

    private static Set<String> puts(Class<?> type) {
        return Arrays.stream(type.getMethods())
                .filter(method -> method.getName().startsWith("put") && !Modifier.isStatic(method.getModifiers()))
                .map(HasherApiTest::signature)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Test
    void hasher_offers_hash4j_s_put_methods_with_types_of_its_own() {
        Set<String> sink = puts(HashSink.class);
        Set<String> hasher = puts(Hasher.class);

        Set<String> missing = new TreeSet<>(sink);
        missing.removeAll(hasher);
        missing.removeAll(LEFT_OUT);
        Set<String> dropped = new TreeSet<>(hasher);
        dropped.removeAll(sink);
        dropped.removeAll(OWN);

        assertThat(missing).as("hash4j's HashSink has put methods Hasher lacks: add them, with hash4j's encoding (and "
                + "to HasherDifferentialTest), or list them as left out").isEmpty();
        assertThat(dropped).as("Hasher has put methods hash4j's HashSink no longer has: keep them, their encoding is "
                + "Hasher's own, and list them as own").isEmpty();
        assertThat(sink).as("still in HashSink").containsAll(LEFT_OUT);
    }

    @Test
    void no_hash4j_type_is_part_of_the_hasher_package_s_api() {
        for (Class<?> type : new Class<?>[]{Hasher.class, Hasher.Funnel.class, Hash.class,
                Hashable.class, HashProvider.class}) {
            assertThat(type.getInterfaces()).as("%s's interfaces", type.getSimpleName())
                    .noneMatch(api -> api.getName().startsWith("com.dynatrace"));
            for (Method method : type.getMethods()) {
                if (method.getDeclaringClass() == Object.class || method.getDeclaringClass() == Record.class
                        || method.getDeclaringClass() == Enum.class) {
                    continue;
                }
                assertThat(method.getReturnType().getName()).as("%s.%s", type.getSimpleName(), method.getName())
                        .doesNotStartWith("com.dynatrace");
                assertThat(method.getParameterTypes()).as("%s.%s", type.getSimpleName(), method.getName())
                        .noneMatch(parameter -> parameter.getName().startsWith("com.dynatrace"));
            }
        }
    }
}
