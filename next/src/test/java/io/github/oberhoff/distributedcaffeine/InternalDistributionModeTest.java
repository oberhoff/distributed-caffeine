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

import com.github.benmanes.caffeine.cache.Expiry;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code withDistributionMode}: {@code INVALIDATE} by default; {@code NONE} subscribes to nothing, so a change made
 * elsewhere reaches a local copy only through Caffeine's expiry, which defaults to the store's maximum age.
 */
@NullMarked
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
class InternalDistributionModeTest {

    private static final Duration MAXIMUM_AGE = Duration.ofHours(1);

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final AtomicInteger subscriptions = new AtomicInteger();
    private final AtomicLong nanos = new AtomicLong();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DistributedCaffeine<String, String> builder() {
        Adapter<String, String> adapter = settings ->
                // the feed whatever the profile's change source: these tests count its subscriptions
                new InternalCoreTest.DelegatingStore(database.openFeed("cache")) {
                    @Override
                    public ChangeSubscription subscribe(ChangeListener listener) {
                        subscriptions.incrementAndGet();
                        return super.subscribe(listener);
                    }
                };
        return DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(MAXIMUM_AGE).withMaximumSize(1_000));
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private DistributedCache<String, String> none(Function<DistributedCaffeine<String, String>, DistributedCaffeine<String, String>> configuration) {
        return track(configuration.apply(builder().withDistributionMode(DistributionMode.NONE)).build());
    }

    @Test
    void invalidation_is_the_default() {
        DistributedCache<String, String> writer = track(builder().build());
        DistributedCache<String, String> holder = track(builder().build());
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        writer.put("k", "new");

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (holder.policy().getIfPresentQuietly("k") != null && System.nanoTime() < deadline) {
            pause(5);
        }
        assertThat(holder.policy().getIfPresentQuietly("k")).as("dropped by the message").isNull();
        assertThat(subscriptions).hasValue(2);
    }

    @Test
    void none_subscribes_to_nothing_and_a_change_elsewhere_reaches_a_copy_only_through_expiry() {
        DistributedCache<String, String> writer = track(builder().build());
        DistributedCache<String, String> holder = none(builder -> builder
                .withCaffeine(caffeine -> caffeine.ticker(nanos::get)));
        assertThat(subscriptions).as("only the writer subscribed").hasValue(1);
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        writer.put("k", "new");
        writer.invalidateAll();
        pause(100);
        assertThat(holder.getIfPresent("k")).as("no message reaches it").isEqualTo("old");

        nanos.addAndGet(MAXIMUM_AGE.plusSeconds(1).toNanos());
        assertThat(holder.getIfPresent("k")).as("expired after the store's maximum age, read again").isNull();
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void none_sees_its_own_writes() {
        DistributedCache<String, String> cache = none(Function.identity());

        cache.put("a", "1");
        cache.putAll(java.util.Map.of("b", "2", "c", "3"));
        cache.asMap().merge("a", "+", String::concat);

        assertThat(cache.getIfPresent("a")).isEqualTo("1+");
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("b")).value()).isEqualTo("2");
    }

    @Test
    void every_copy_expires_at_the_maximum_age_at_the_latest() {
        DistributedCache<String, String> defaulted = none(Function.identity());
        assertThat(defaulted.policy().expireAfterWrite().orElseThrow().getExpiresAfter()).isEqualTo(MAXIMUM_AGE);

        DistributedCache<String, String> configured = none(builder -> builder
                .withCaffeine(caffeine -> caffeine.expireAfterWrite(Duration.ofMinutes(5))));
        assertThat(configured.policy().expireAfterWrite().orElseThrow().getExpiresAfter())
                .as("a shorter one is kept").isEqualTo(Duration.ofMinutes(5));

        DistributedCache<String, String> longer = none(builder -> builder
                .withCaffeine(caffeine -> caffeine.expireAfterWrite(MAXIMUM_AGE.multipliedBy(2))));
        assertThat(longer.policy().expireAfterWrite().orElseThrow().getExpiresAfter())
                .as("a longer one is lowered to it").isEqualTo(MAXIMUM_AGE);

        DistributedCache<String, String> variable = none(builder -> builder.withCaffeine(caffeine -> caffeine
                .expireAfter(Expiry.writing((String key, String value) -> Duration.ofMinutes(2)))));
        assertThat(variable.policy().expireVariably()).isPresent();
        assertThat(variable.policy().expireAfterWrite()).isEmpty();

        DistributedCache<String, String> accessOnly = none(builder -> builder
                .withCaffeine(caffeine -> caffeine.expireAfterAccess(Duration.ofMinutes(10))));
        assertThat(accessOnly.policy().expireAfterAccess()).isPresent();
        assertThat(accessOnly.policy().expireAfterWrite().orElseThrow().getExpiresAfter())
                .as("access alone does not bound the age").isEqualTo(MAXIMUM_AGE);

        DistributedCache<String, String> distributed = track(builder().build());
        assertThat(distributed.policy().expireAfterWrite().orElseThrow().getExpiresAfter())
                .as("with distribution too: a message dropping it may be lost").isEqualTo(MAXIMUM_AGE);
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // the null mode is the point: it is rejected
    void the_mode_is_set_once_and_a_builder_with_none_builds_again() {
        DistributedCaffeine<String, String> builder = builder().withDistributionMode(DistributionMode.NONE);
        assertThatThrownBy(() -> builder.withDistributionMode(DistributionMode.INVALIDATE))
                .isInstanceOf(IllegalStateException.class);
        DistributedCaffeine<String, String> fresh = builder();
        assertThatThrownBy(() -> fresh.withDistributionMode(null)).isInstanceOf(NullPointerException.class);

        // one open cache per adapter: the first is closed before the builder builds again
        builder.build().close();
        DistributedCache<String, String> second = track(builder.build());
        assertThat(second.policy().expireAfterWrite()).isPresent();
    }
}
