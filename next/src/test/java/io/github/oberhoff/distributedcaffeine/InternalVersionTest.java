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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The core's versions, a hybrid logical clock: every version proposed is larger than every one proposed or seen
 * before, positive, near the time; {@link InternalVersion#UNCONFIRMED} below all of them, {@link InternalVersion#ANYTHING}
 * above.
 */
@NullMarked
class InternalVersionTest {

    private final InternalVersion clock = new InternalVersion();

    @Test
    void every_version_proposed_is_larger_than_the_one_before() {
        long last = 0;
        for (int index = 0; index < 100_000; index++) {
            long version = clock.fresh();
            assertThat(version).isGreaterThan(last).isPositive().isLessThan(InternalVersion.ANYTHING);
            last = version;
        }
    }

    @Test
    void a_version_seen_is_passed_by_the_next_one_proposed() {
        long ahead = clock.fresh() + (60_000L << 20); // a minute ahead, as from a clock ahead
        clock.observe(ahead);

        assertThat(clock.fresh()).isGreaterThan(ahead);
        assertThat(clock.next(ahead + 5)).isGreaterThan(ahead + 5);
    }

    @Test
    void a_version_seen_that_is_behind_changes_nothing() {
        long before = clock.fresh();
        clock.observe(before - (1_000L << 20));

        assertThat(clock.fresh()).isGreaterThan(before);
    }

    @Test
    void each_instance_has_a_clock_of_its_own_moved_only_by_what_it_sees() {
        InternalVersion other = new InternalVersion();
        long ahead = other.fresh() + (60_000L << 20);
        other.observe(ahead);

        assertThat(clock.fresh()).as("not seen here").isLessThan(ahead);
        clock.observe(other.fresh());
        assertThat(clock.fresh()).as("seen").isGreaterThan(ahead);
    }

    @Test
    void a_version_more_than_a_minute_ahead_is_reported_and_followed() {
        List<Long> reported = new ArrayList<>();
        InternalVersion reporting = new InternalVersion(reported::add);
        long ahead = reporting.fresh() + (TimeUnit.MINUTES.toMillis(2) << 20);

        reporting.observe(ahead);

        assertThat(reported).singleElement().satisfies(millis -> assertThat(millis)
                .isBetween(TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(3)));
        assertThat(reporting.fresh()).as("followed").isGreaterThan(ahead);
    }

    @Test
    void a_version_more_than_a_day_ahead_is_not_followed_but_a_write_on_it_goes_above() {
        List<Long> reported = new ArrayList<>();
        InternalVersion reporting = new InternalVersion(reported::add);
        long farAhead = reporting.fresh() + (TimeUnit.DAYS.toMillis(365) << 20);

        long onIt = reporting.next(farAhead);

        assertThat(reported).as("reported").hasSize(1);
        assertThat(onIt).as("a write decided on it goes above it").isGreaterThan(farAhead);
        assertThat(reporting.fresh()).as("the clock stays near its own time").isLessThan(farAhead);
    }

    @Test
    void the_end_of_the_range_is_an_error_not_a_version_repeated() {
        // the largest version below ANYTHING: nothing lies above it
        assertThatThrownBy(() -> clock.next(InternalVersion.ANYTHING - 1))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("end of their range");
    }

    @Test
    void versions_are_ordered_as_numbers() {
        long first = clock.fresh();
        long second = clock.fresh();

        assertThat(InternalVersion.newer(second, first)).isTrue();
        assertThat(InternalVersion.atLeast(first, second)).isFalse();
        assertThat(InternalVersion.atLeast(first, first)).isTrue();
        assertThat(InternalVersion.atLeastBoth(first, second)).isEqualTo(second);
    }

    @Test
    void an_unconfirmed_value_is_below_every_version_proposed_and_anything_above() {
        long version = clock.fresh();

        assertThat(InternalVersion.atLeast(version, InternalVersion.UNCONFIRMED)).isTrue();
        assertThat(InternalVersion.atLeast(InternalVersion.UNCONFIRMED, version)).isFalse();
        assertThat(InternalVersion.atLeast(version, InternalVersion.ANYTHING)).isFalse();
        assertThat(InternalVersion.atLeastBoth(version, InternalVersion.ANYTHING)).isEqualTo(InternalVersion.ANYTHING);
    }
}
