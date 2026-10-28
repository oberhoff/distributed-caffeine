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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The core's versions: ordered within an incarnation, unordered across incarnations, incarnation 0 below every
 * other, and {@link InternalVersion#ANYTHING} above every version.
 */
class InternalVersionTest {

    @Test
    void a_fresh_version_is_positive_and_starts_an_incarnation_of_its_own() {
        for (int index = 0; index < 10_000; index++) {
            long version = InternalVersion.fresh();
            assertThat(version).isPositive().isLessThan(InternalVersion.ANYTHING);
            assertThat(InternalVersion.incarnation(version)).isNotZero();
            assertThat(version & ((1L << InternalVersion.COUNTER_BITS) - 1)).isEqualTo(1);
        }
    }

    @Test
    void the_next_version_continues_the_incarnation_and_is_newer() {
        long first = InternalVersion.fresh();
        long second = InternalVersion.next(first);

        assertThat(InternalVersion.incarnation(second)).isEqualTo(InternalVersion.incarnation(first));
        assertThat(InternalVersion.newer(second, first)).isTrue();
        assertThat(InternalVersion.atLeast(first, second)).isFalse();
        assertThat(InternalVersion.atLeast(first, first)).isTrue();
    }

    @Test
    void an_exhausted_counter_starts_a_new_incarnation() {
        long last = InternalVersion.fresh() | ((1L << InternalVersion.COUNTER_BITS) - 1);

        long next = InternalVersion.next(last);

        assertThat(InternalVersion.incarnation(next)).isNotEqualTo(InternalVersion.incarnation(last));
        assertThat(InternalVersion.atLeast(next, last)).isFalse();
        assertThat(InternalVersion.atLeast(last, next)).isFalse();
    }

    @Test
    void two_incarnations_are_not_ordered_either_way() {
        long one = InternalVersion.fresh();
        long other = InternalVersion.next(InternalVersion.next(InternalVersion.fresh()));

        assertThat(InternalVersion.atLeast(one, other)).isFalse();
        assertThat(InternalVersion.atLeast(other, one)).isFalse();
        assertThat(InternalVersion.atLeastBoth(one, other)).isEqualTo(InternalVersion.ANYTHING);
    }

    @Test
    void the_newer_of_one_incarnation_is_at_least_both() {
        long first = InternalVersion.fresh();
        long second = InternalVersion.next(first);

        assertThat(InternalVersion.atLeastBoth(first, second)).isEqualTo(second);
        assertThat(InternalVersion.atLeastBoth(second, first)).isEqualTo(second);
    }

    @Test
    void an_unconfirmed_value_is_below_every_stored_version() {
        long stored = InternalVersion.fresh();

        assertThat(InternalVersion.atLeast(stored, InternalVersion.UNCONFIRMED)).isTrue();
        assertThat(InternalVersion.atLeast(InternalVersion.UNCONFIRMED, stored)).isFalse();
        assertThat(InternalVersion.atLeastBoth(InternalVersion.UNCONFIRMED, stored)).isEqualTo(stored);
    }

    @Test
    void anything_is_above_every_version_and_nothing_is_at_least_it() {
        long stored = InternalVersion.fresh();

        assertThat(InternalVersion.atLeast(stored, InternalVersion.ANYTHING)).isFalse();
        assertThat(InternalVersion.atLeast(InternalVersion.ANYTHING, stored)).isTrue();
        assertThat(InternalVersion.atLeastBoth(stored, InternalVersion.ANYTHING)).isEqualTo(InternalVersion.ANYTHING);
    }
}
