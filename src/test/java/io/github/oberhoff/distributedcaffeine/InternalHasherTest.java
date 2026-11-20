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

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The runtime check of the hash library ({@link InternalHasher#verify}): its known answers are the stored format's,
 * which HasherBuiltInTest and HasherTest pin against oracles of their own.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class InternalHasherTest {

    @ParameterizedTest
    @EnumSource(InternalHasher.KnownAnswer.class)
    void the_runtime_check_s_known_answers_are_the_format_s(InternalHasher.KnownAnswer answer) {
        assertThat(answer.compute()).isEqualTo(answer.expected);
    }

    @Test
    void the_runtime_check_passes() {
        InternalHasher.verify();
    }

    @Test
    void a_mismatch_names_where_hash4j_was_loaded_from() {
        // hash4j's jar records no version, so the message names the jar, whose name carries it
        assertThat(InternalHasher.hash4jLocation()).contains("hash4j");
    }
}
