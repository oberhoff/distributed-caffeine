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
package io.github.oberhoff.distributedcaffeine.spi;

import io.github.oberhoff.distributedcaffeine.spi.StoreMarker.Relation;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The newer version wins: how a place's marker relates to this version's ({@link StoreMarker}).
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class StoreMarkerTest {

    static Stream<Arguments> markers() {
        return Stream.of(
                Arguments.of(StoreMarker.CURRENT, Relation.SAME),
                Arguments.of("distributed-caffeine schema=1 hash=xxh3-128/1", Relation.SAME),
                // markers without pairs, of the versions before the hash format (and Redis's bare layout number)
                Arguments.of("distributed-caffeine schema 1", Relation.EARLIER),
                Arguments.of("1", Relation.EARLIER),
                Arguments.of("distributed-caffeine schema 2", Relation.LATER),
                Arguments.of("2", Relation.LATER),
                // ordered by the schema, then by the hash format's revision
                Arguments.of("distributed-caffeine schema=1 hash=xxh3-128/0", Relation.EARLIER),
                Arguments.of("distributed-caffeine schema=0 hash=xxh3-128/9", Relation.EARLIER),
                Arguments.of("distributed-caffeine schema=1 hash=xxh3-128/2", Relation.LATER),
                Arguments.of("distributed-caffeine schema=2 hash=xxh3-128/1", Relation.LATER),
                Arguments.of("distributed-caffeine schema=2 hash=xxh3-128/0", Relation.LATER),
                // what this version cannot order or read: a later one wrote it
                Arguments.of("distributed-caffeine schema=1 hash=other-128/1", Relation.LATER),
                Arguments.of("distributed-caffeine schema=1 hash=xxh3-128", Relation.LATER),
                Arguments.of("distributed-caffeine schema=1 hash=xxh3-128/1 keys=binary", Relation.LATER),
                Arguments.of("distributed-caffeine schema=1", Relation.LATER),
                Arguments.of("distributed-caffeine schema=1  hash=xxh3-128/1", Relation.LATER),
                Arguments.of("distributed-caffeine schema=1 schema=1 hash=xxh3-128/1", Relation.LATER),
                Arguments.of("distributed-caffeine something else", Relation.LATER),
                // none of this library's
                Arguments.of(null, Relation.FOREIGN),
                Arguments.of("", Relation.FOREIGN),
                Arguments.of("a table created by hand", Relation.FOREIGN),
                Arguments.of("distributed-caffeine", Relation.FOREIGN));
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("markers")
    void a_marker_relates_to_this_version_s(@Nullable String marker, Relation expected) {
        assertThat(StoreMarker.relationOf(marker)).isEqualTo(expected);
    }
}
