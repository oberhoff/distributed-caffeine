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

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

// The synchronization lock, striped by key so that writes to different keys no longer wait for one another.
// The critical section here is a round-trip to the data store, so a single lock made the whole cache instance - and
// every one of its writes - one serialization point. Striping keeps that within a key (which is what ordering
// between the instances rests on) but not across keys (which is what made it cost so much): two writes to different
// keys now reach the store at the same time.
// What cannot be striped is everything that concerns the cache instance as a whole rather than one of its keys -
// invalidating all, clearing it, reconciling it against the store, activating or deactivating it - because those
// have to be atomic against every key of it. Those take a global (write) lock; every key operation takes the shared
// (read) side of it plus the one or more stripes its keys fall into. A global operation therefore excludes all key
// operations, and key operations exclude only the global ones and each other where they touch the same stripe.
// Deliberately reentrant, as the single lock was, because the cache facade and its views nest calls on the same key
// (putIfAbsent taking put, compute taking put/remove, ...) - which now nest onto the same stripe.
// What a key operation can no longer do is nest anything beyond its own stripes, which the single lock allowed: the
// shared side cannot be upgraded to the exclusive one, so nesting a whole-cache operation would wait for itself
// forever, and nesting another stripe takes two stripes in an order no other thread is bound to, so two threads
// doing so the other way round would wait for each other forever. Both are refused instead (see
// refuseNestedStripes) - the same contract Caffeine has for a compute, whose function must not modify the cache.
// Fairness costs something worth knowing before taking a stripe in a loop: handoff alternates, so whoever acquires
// it once per item gets exactly one item per store round-trip of whoever else is waiting, however much work is
// pending. Measured with the single lock and one writer contending, handing inbound cache entries over one at a time
// held delivery to the writer's own rate, while handing them over in batches was two orders of magnitude faster.
// Anything arriving in quantity is therefore to be taken one stripe at a time (see runLockedPerStripe), not one item
// per acquisition
class InternalSynchronizationLock {

    // A power of two, the top bits of the key's spread hash. More stripes than concurrent writers is what matters
    // (collisions are what costs), and there is nothing per stripe but a lock, so this is deliberately generous.
    // Measured against 64 stripes on single-node and three-node replica sets: throughput the same within noise, but
    // the 99th percentile lower in every case, by up to a third at 64 concurrent writers - for about 10 KB of locks
    private static final int STRIPE_BITS = 8;
    static final int STRIPE_COUNT = 1 << STRIPE_BITS; // 2^8 = 256 stripes
    // the golden ratio as a 32-bit fraction (Fibonacci hashing), see stripeIndexOf
    private static final int SPREAD = 0x9E3779B9;

    // Shared (read) for the key operations, exclusive (write) for the whole-cache ones. Fair on both sides so that a
    // whole-cache operation - which is rare but must not wait behind a stream of key operations - is handed the lock
    // as soon as the key operations holding it are done, rather than being overtaken by whichever key operation
    // arrives next. The key operations pay a little for that, which the store round-trip they carry hides
    private final ReentrantReadWriteLock scopeLock;
    private final ReentrantLock[] stripeLocks;

    InternalSynchronizationLock() {
        scopeLock = new ReentrantReadWriteLock(true);
        stripeLocks = new ReentrantLock[STRIPE_COUNT];
        for (int stripe = 0; stripe < STRIPE_COUNT; stripe++) {
            // fair for the same reason the single lock was: the critical section is a store round-trip, and a barging
            // lock lets whoever just released re-acquire ahead of the queue and starve the waiting ones. Measured with
            // the single lock at 16 concurrent writers, barging cost over a second in the worst case against 38 ms fair
            stripeLocks[stripe] = new ReentrantLock(true);
        }
    }

    // ---- the whole cache ----

    void runLocked(Runnable runnable) {
        lockExclusive();
        try {
            runnable.run();
        } finally {
            scopeLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unused")
    <T> T getLocked(Supplier<T> supplier) {
        lockExclusive();
        try {
            return supplier.get();
        } finally {
            scopeLock.writeLock().unlock();
        }
    }

    // same as above, but for suppliers whose result may be null - not delegating to it, because its type variable
    // cannot carry a nullable result
    <T> @Nullable T getLockedOrNull(Supplier<@Nullable T> supplier) {
        lockExclusive();
        try {
            return supplier.get();
        } finally {
            scopeLock.writeLock().unlock();
        }
    }

    private void lockExclusive() {
        if (isInKeyScope()) {
            throw new IllegalStateException(
                    "An operation on the whole cache cannot be carried out from within an operation on a key");
        }
        scopeLock.writeLock().lock();
    }

    // ---- one key ----

    void runLocked(InternalKey<?> key, Runnable runnable) {
        getLockedOrNull(stripeLocks[stripeIndexOf(key)], () -> {
            runnable.run();
            return null;
        });
    }

    <T> @Nullable T getLockedOrNull(InternalKey<?> key, Supplier<@Nullable T> supplier) {
        return getLockedOrNull(stripeLocks[stripeIndexOf(key)], supplier);
    }

    private <T> @Nullable T getLockedOrNull(ReentrantLock stripe, Supplier<@Nullable T> supplier) {
        refuseNestedStripes(stripe);
        scopeLock.readLock().lock();
        try {
            stripe.lock();
            try {
                return supplier.get();
            } finally {
                stripe.unlock();
            }
        } finally {
            scopeLock.readLock().unlock();
        }
    }

    // ---- one or more keys ----

    // Several keys in one operation (a bulk put, a bulk invalidation, the apply of a bulk load or of a bulk refresh).
    // The distinct stripes are taken in ascending order, all before the body runs: two such operations overlapping in
    // some of their keys would otherwise be free to take them in opposite orders and deadlock
    void runLocked(Collection<? extends InternalKey<?>> keys, Runnable runnable) {
        ReentrantLock[] stripes = distinctStripesOf(keys);
        refuseNestedStripes(stripes);
        scopeLock.readLock().lock();
        int locked = 0;
        try {
            for (; locked < stripes.length; locked++) {
                stripes[locked].lock();
            }
            runnable.run();
        } finally {
            // release in reverse, and only what was actually taken: were a stripe's lock to fail part way through, the
            // ones before it are still given back
            for (int position = locked - 1; position >= 0; position--) {
                stripes[position].unlock();
            }
            scopeLock.readLock().unlock();
        }
    }

    // Items of possibly many keys, applied one stripe at a time: grouped by the stripe of their key, in the order
    // given within each group, and every group under its stripe alone. So each stripe is taken once for all of its
    // items rather than once per item (see above for why that matters), and no stripe is held while waiting for
    // another one. The stripes free right now go first, without waiting, and only then is waited for the busy
    // ones - so a writer on one stripe holds up the items of that stripe alone, not those of the stripes after it.
    // Trying a fair lock does not queue behind its waiters, which is the point here and harmless: it is one attempt
    // per stripe, not a loop that could starve them.
    // What this gives up is the order between items of different stripes - and so between different keys - which is
    // why it is only for items whose order matters per key alone
    <E> void runLockedPerStripe(List<E> items, Function<? super E, ? extends InternalKey<?>> keyOf,
                                Consumer<? super E> action) {
        Map<Integer, List<E>> itemsByStripe = new LinkedHashMap<>();
        for (E item : items) {
            itemsByStripe.computeIfAbsent(stripeIndexOf(keyOf.apply(item)), stripe -> new ArrayList<>()).add(item);
        }
        ReentrantLock[] stripes = itemsByStripe.keySet().stream()
                .map(stripe -> stripeLocks[stripe])
                .toArray(ReentrantLock[]::new);
        refuseNestedStripes(stripes);
        scopeLock.readLock().lock();
        try {
            List<List<E>> busy = new ArrayList<>();
            List<ReentrantLock> busyStripes = new ArrayList<>();
            int position = 0;
            for (List<E> stripeItems : itemsByStripe.values()) {
                ReentrantLock stripe = stripes[position++];
                if (stripe.tryLock()) {
                    applyUnder(stripe, stripeItems, action);
                } else {
                    busy.add(stripeItems);
                    busyStripes.add(stripe);
                }
            }
            for (int index = 0; index < busy.size(); index++) {
                ReentrantLock stripe = busyStripes.get(index);
                stripe.lock();
                applyUnder(stripe, busy.get(index), action);
            }
        } finally {
            scopeLock.readLock().unlock();
        }
    }

    // applies the items with their stripe already taken, and gives it back
    private static <E> void applyUnder(ReentrantLock stripe, List<E> stripeItems, Consumer<? super E> action) {
        try {
            stripeItems.forEach(action);
        } finally {
            stripe.unlock();
        }
    }

    private ReentrantLock[] distinctStripesOf(Collection<? extends InternalKey<?>> keys) {
        if (keys.size() == 1) {
            return new ReentrantLock[]{stripeLocks[stripeIndexOf(keys.iterator().next())]};
        }
        int[] indices = new int[keys.size()];
        int count = 0;
        for (InternalKey<?> key : keys) {
            int index = stripeIndexOf(key);
            if (!contains(indices, count, index)) {
                indices[count++] = index;
            }
        }
        Arrays.sort(indices, 0, count);
        ReentrantLock[] stripes = new ReentrantLock[count];
        for (int position = 0; position < count; position++) {
            stripes[position] = stripeLocks[indices[position]];
        }
        return stripes;
    }

    // whether the stripe is among those collected so far - at most STRIPE_COUNT of them, since that is how many
    // distinct stripes there are, so this stays cheap however many keys an operation carries
    private static boolean contains(int[] indices, int count, int index) {
        for (int position = 0; position < count; position++) {
            if (indices[position] == index) {
                return true;
            }
        }
        return false;
    }

    // ---- nesting ----

    // Whether this thread is inside a key operation and not inside a whole-cache one. Holding the exclusive side
    // nests anything: no other thread holds a stripe meanwhile, so whatever is taken from within it cannot wait for
    // one
    private boolean isInKeyScope() {
        return scopeLock.getReadHoldCount() > 0 && !scopeLock.isWriteLockedByCurrentThread();
    }

    // Nesting within a key operation is refused for any stripe this thread does not hold already, before waiting for
    // anything. Decided by stripe rather than by key, because a stripe already held is all that matters for whether
    // nesting can deadlock - which means a nested operation on another key passes where that key happens to share the
    // stripe, and is refused everywhere else
    private void refuseNestedStripes(ReentrantLock... stripes) {
        if (isInKeyScope()) {
            for (ReentrantLock stripe : stripes) {
                if (!stripe.isHeldByCurrentThread()) {
                    throw new IllegalStateException(
                            "An operation on a key cannot be carried out from within an operation on another key");
                }
            }
        }
    }

    // The stripe a key falls into, from the key's own hash code - so that equal keys share a stripe whichever key
    // instance they arrive as, and a key's stripe never changes. Spread by Fibonacci hashing and taken from the top
    // bits, because a hash code is only as good as the key type makes it: masking the low bits (even after folding
    // the high ones in, as HashMap does) put a thousand Long keys that are multiples of 64 into 4 of the 256 stripes,
    // and a thousand Integer keys that are multiples of 1024 into 16 of them, while this spreads both over all 256
    static int stripeIndexOf(InternalKey<?> key) {
        return (key.hashCode() * SPREAD) >>> (Integer.SIZE - STRIPE_BITS);
    }

    // The assertions of the lock that is required, by what it covers rather than merely that one is held: with a
    // single lock, one being held at all meant that everything was excluded, while a stripe being held now excludes
    // only its own keys and the shared side excludes nothing but the whole cache. Deliberately whichever thread holds
    // it rather than a check of this thread's holding: the invariant to assert is that the lock is active, not that
    // this thread is the holder.
    // For a write to keys: the stripes of all of them, or the whole cache, under which no stripe is taken
    void ensureLock(Collection<? extends InternalKey<?>> keys) {
        if (scopeLock.isWriteLocked()) {
            return;
        }
        for (InternalKey<?> key : keys) {
            if (!stripeLocks[stripeIndexOf(key)].isLocked()) {
                throw new IllegalStateException("No synchronization lock found for the keys written");
            }
        }
    }

    // for what concerns the whole cache, which nothing but the exclusive side covers
    void ensureExclusiveLock() {
        if (!scopeLock.isWriteLocked()) {
            throw new IllegalStateException("No exclusive synchronization lock found");
        }
    }
}
