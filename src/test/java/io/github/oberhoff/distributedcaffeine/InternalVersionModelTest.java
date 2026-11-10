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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A model of the version protocol, every reachable state searched (VISION.md, "In-memory store profiles", dimension
 * 4): an abstraction of the core's rules small enough to explore exhaustively, where the other tests sample. One key,
 * a store with a replica, two instances; versions are plain numbers, and two instances may propose the same one (the
 * real format makes that one in 1024 within a millisecond and counter: the model takes the worst case, so that it
 * finds a repeated version sooner than reality would).
 * <ul>
 *     <li>The store assigns versions as Redis' script does ({@code max(stored + 1, floor, min(proposal, floor +
 *     lead))}, the floor its clock), or applies a proposal above the stored version, as the conditional stores do (a
 *     refused writer learns the stored version and proposes above it).</li>
 *     <li>An instance holds a copy (version, value, trusted or not), a queue of what the store announces (a version
 *     written, or a gap), its wall clock and the largest version it saw; it proposes above both.</li>
 *     <li>The steps: a write by an instance, a read (a trusted copy served, an untrusted one checked against the
 *     store, a miss loaded), a message delivered (a larger version drops the copy, a smaller one has it checked, an
 *     equal one is the same write), the clocks moving, the replica catching up, a failover (the row back to the
 *     replica's, the clock going on or standing where the replica's stood, a gap for every instance in place of what
 *     was not delivered), an instance restarting (forgetting what it saw), and an atomic method deciding on the copy
 *     not to write ({@code replace} with the value the store holds as the expected one), confirmed by a read.</li>
 * </ul>
 * Checked in every state and step: a trusted copy with nothing pending is the store's row; a read with nothing pending
 * answers the store's value; a decision not to write is never confirmed while the store holds the expected value.
 * Checking a copy by its version alone breaks them (the version repeating with another value after a failover, found
 * 2026-11-10); by its version and value, as the core does (InternalCore#stillStored, #sameContent), none breaks, in
 * about a million states per way of assigning versions.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(120)
class InternalVersionModelTest {

    // the bounds of the search
    private static final int INSTANCES = 2;
    private static final int WRITES = 3;
    private static final int CLOCK = 2;
    private static final int LEAD = 1;
    private static final int FAILOVERS = 1;
    private static final int RESTARTS = 1;
    // a gap in an instance's queue, where versions are positive
    private static final int GAP = -1;

    @ParameterizedTest
    @EnumSource(Assignment.class)
    void checking_a_copy_by_its_version_and_value_keeps_every_instance_right_in_every_reachable_state(
            Assignment assignment) {
        Result result = new Model(assignment, Check.VERSION_AND_VALUE).search();

        assertThat(result.violation()).as("in %,d states: %s", result.states(), result.trace()).isNull();
        assertThat(result.states()).as("the search covered the failovers and restarts").isGreaterThan(100_000);
    }

    // the model's own check: comparing versions only, the search finds the case found 2026-11-10 on Valkey (otherwise
    // the model would be too coarse to tell anything)
    @ParameterizedTest
    @EnumSource(Assignment.class)
    void checking_a_copy_by_its_version_alone_is_found_wrong(Assignment assignment) {
        Result result = new Model(assignment, Check.VERSION_ONLY).search();

        assertThat(result.violation()).as("in %,d states", result.states()).isNotNull();
        assertThat(result.trace()).anyMatch(step -> step.startsWith("failover"));
    }

    enum Assignment {
        // the store's version: max(stored + 1, floor, min(proposal, floor + lead)) (Redis, Valkey)
        ASSIGNED,
        // the proposal, above the stored version (MariaDB, MySQL)
        CONDITIONAL
    }

    enum Check {
        // a copy confirmed by its version alone: the core before 2026-11-10
        VERSION_ONLY,
        // by its version and value (InternalCore#stillStored, #sameContent)
        VERSION_AND_VALUE
    }

    record Row(int version, int value) {
    }

    record Copy(int version, int value, boolean trusted) {

        Copy distrusted() {
            return new Copy(version, value, false);
        }

        Copy confirmed() {
            return new Copy(version, value, true);
        }
    }

    // queue: what the store announced and the instance has not processed yet; seen: the largest version it saw
    record Instance(@Nullable Copy copy, List<Integer> queue, int clock, int seen) {

        static final Instance STARTED = new Instance(null, List.of(), 0, 0);
    }

    record State(@Nullable Row row, @Nullable Row replica, int storeClock, int replicaClock, List<Instance> instances,
                 int writes, int failovers, int restarts) {

        State with(int index, Instance instance) {
            List<Instance> changed = new ArrayList<>(instances);
            changed.set(index, instance);
            return withInstances(changed);
        }

        State withStore(@Nullable Row changedRow, int changedClock) {
            return new State(changedRow, replica, changedClock, replicaClock, instances, writes, failovers, restarts);
        }

        State withReplica(@Nullable Row changedReplica, int changedClock) {
            return new State(row, changedReplica, storeClock, changedClock, instances, writes, failovers, restarts);
        }

        State withCounts(int changedWrites, int changedFailovers, int changedRestarts) {
            return new State(row, replica, storeClock, replicaClock, instances, changedWrites, changedFailovers,
                    changedRestarts);
        }

        State withInstances(List<Instance> changed) {
            return new State(row, replica, storeClock, replicaClock, List.copyOf(changed), writes, failovers,
                    restarts);
        }

        // nothing announced to the instance waits to be processed
        boolean settled(int index) {
            return instances.get(index).queue().isEmpty();
        }
    }

    // a step and the state it leads to, or the violation it found
    record Step(String name, @Nullable State next, @Nullable String violation) {

        static Step to(String name, State next) {
            return new Step(name, next, null);
        }

        static Step violating(String name, String violation) {
            return new Step(name, null, violation);
        }
    }

    record Result(int states, @Nullable String violation, List<String> trace) {
    }

    // the protocol under one way of assigning versions and of checking a copy
    private record Model(Assignment assignment, Check check) {

        // breadth first, so that a violation comes with a shortest trace to it
        Result search() {
            State initial = new State(null, null, 0, 0, Collections.nCopies(INSTANCES, Instance.STARTED), 0, 0, 0);
            Map<State, Map.Entry<State, String>> parents = new HashMap<>();
            parents.put(initial, Map.entry(initial, ""));
            Queue<State> frontier = new ArrayDeque<>();
            frontier.add(initial);
            while (!frontier.isEmpty()) {
                State state = frontier.remove();
                String broken = invariant(state);
                if (broken != null) {
                    return new Result(parents.size(), broken, trace(parents, state, null));
                }
                for (Step step : steps(state)) {
                    State next = step.next();
                    if (next == null) {
                        return new Result(parents.size(), step.violation(), trace(parents, state, step.name()));
                    }
                    if (!parents.containsKey(next)) {
                        parents.put(next, Map.entry(state, step.name()));
                        frontier.add(next);
                    }
                }
            }
            return new Result(parents.size(), null, List.of());
        }

        private static List<String> trace(Map<State, Map.Entry<State, String>> parents, State last,
                                          @Nullable String finalStep) {
            List<String> steps = new ArrayList<>();
            if (finalStep != null) {
                steps.add(finalStep);
            }
            Map.Entry<State, String> parent = parents.get(last);
            while (parent != null && !parent.getValue().isEmpty()) {
                steps.add(parent.getValue());
                parent = parents.get(parent.getKey());
            }
            Collections.reverse(steps);
            return steps;
        }

        // a trusted copy with nothing pending is the store's row: every write announces itself, and a failover
        // leaves a gap
        private static @Nullable String invariant(State state) {
            for (int index = 0; index < INSTANCES; index++) {
                Copy copy = state.instances().get(index).copy();
                Row row = state.row();
                if (state.settled(index) && copy != null && copy.trusted()
                        && (row == null || row.version() != copy.version() || row.value() != copy.value())) {
                    return "instance " + index + " trusts " + copy + " with nothing pending, the store holds " + row;
                }
            }
            return null;
        }

        private List<Step> steps(State state) {
            List<Step> steps = new ArrayList<>();
            for (int index = 0; index < INSTANCES; index++) {
                Instance instance = state.instances().get(index);
                if (state.writes() < WRITES) {
                    steps.add(write(state, index));
                }
                steps.add(read(state, index));
                if (!instance.queue().isEmpty()) {
                    steps.add(deliver(state, index));
                }
                Step decision = decideNotToWrite(state, index);
                if (decision != null) {
                    steps.add(decision);
                }
                if (instance.clock() < CLOCK) {
                    steps.add(Step.to("instance " + index + " clock ticks", state.with(index,
                            new Instance(instance.copy(), instance.queue(), instance.clock() + 1, instance.seen()))));
                }
                if (state.restarts() < RESTARTS) {
                    steps.add(Step.to("instance " + index + " restarts", state.with(index,
                            new Instance(null, List.of(), instance.clock(), 0))
                            .withCounts(state.writes(), state.failovers(), state.restarts() + 1)));
                }
            }
            if (state.storeClock() < CLOCK) {
                steps.add(Step.to("store clock ticks", state.withStore(state.row(), state.storeClock() + 1)));
            }
            if (!Objects.equals(state.row(), state.replica()) || state.replicaClock() != state.storeClock()) {
                steps.add(Step.to("replica catches up", state.withReplica(state.row(), state.storeClock())));
            }
            if (state.failovers() < FAILOVERS) {
                steps.add(failover(state, false));
                steps.add(failover(state, true));
            }
            return steps;
        }

        // a write: the store's version (assigned, or the proposal above the stored one), announced to every
        // instance, the writer's own included; the writer holds what it wrote
        private Step write(State state, int index) {
            Instance writer = state.instances().get(index);
            int value = state.writes() + 1;
            int stored = state.row() == null ? 0 : state.row().version();
            int proposal = Math.max(writer.clock(), writer.seen() + 1);
            int version = assignment == Assignment.ASSIGNED
                    ? Math.max(stored + 1, Math.max(state.storeClock(),
                    Math.min(proposal, state.storeClock() + LEAD)))
                    // refused at or below the stored version: the writer learns it and proposes above
                    : Math.max(proposal, stored + 1);
            List<Instance> instances = new ArrayList<>();
            for (int other = 0; other < INSTANCES; other++) {
                Instance instance = state.instances().get(other);
                List<Integer> queue = new ArrayList<>(instance.queue());
                queue.add(version);
                Copy copy = other == index ? new Copy(version, value, true) : instance.copy();
                int seen = other == index ? Math.max(instance.seen(), version) : instance.seen();
                instances.add(new Instance(copy, List.copyOf(queue), instance.clock(), seen));
            }
            return Step.to("instance " + index + " writes " + value + " at " + version,
                    state.withStore(new Row(version, value), state.storeClock()).withInstances(instances)
                            .withCounts(state.writes() + 1, state.failovers(), state.restarts()));
        }

        // a read: a trusted copy served as it is; an untrusted one checked, kept if the store still has it; a miss
        // loaded. Its answer, with nothing pending, is the store's value
        private Step read(State state, int index) {
            Instance instance = state.instances().get(index);
            Copy copy = instance.copy();
            Row row = state.row();
            Copy after;
            if (copy != null && copy.trusted()) {
                after = copy;
            } else if (copy != null && stillStored(copy, row)) {
                after = copy.confirmed();
            } else {
                after = row == null ? null : new Copy(row.version(), row.value(), true);
            }
            Integer answer = after == null ? null : after.value();
            Integer stored = row == null ? null : row.value();
            String name = "instance " + index + " reads " + answer;
            if (state.settled(index) && !Objects.equals(answer, stored)) {
                return Step.violating(name, "instance " + index + " reads " + answer
                        + " with nothing pending, the store holds " + row);
            }
            // what the store answered is seen
            boolean asked = copy == null || !copy.trusted();
            int seen = asked && row != null ? Math.max(instance.seen(), row.version()) : instance.seen();
            return Step.to(name, state.with(index, new Instance(after, instance.queue(), instance.clock(), seen)));
        }

        // the next announcement: a gap distrusts the copy; a larger version drops it, a smaller one has it checked,
        // an equal one is the write the copy holds
        private Step deliver(State state, int index) {
            Instance instance = state.instances().get(index);
            int head = instance.queue().get(0);
            List<Integer> rest = List.copyOf(instance.queue().subList(1, instance.queue().size()));
            Copy copy = instance.copy();
            Copy after = copy;
            if (copy != null) {
                if (head == GAP) {
                    after = copy.distrusted();
                } else if (head > copy.version()) {
                    after = null;
                } else if (head < copy.version()) {
                    after = stillStored(copy, state.row()) ? copy : null;
                }
            }
            int seen = head == GAP ? instance.seen() : Math.max(instance.seen(), head);
            return Step.to("instance " + index + " receives " + (head == GAP ? "a gap" : "version " + head),
                    state.with(index, new Instance(after, rest, instance.clock(), seen)));
        }

        // replace with the store's value as the expected one, decided on a trusted copy holding another value:
        // nothing to write, so the decision is confirmed by a read (InternalCore, the update loop); confirmed, it
        // answers false although the store held the expected value
        private @Nullable Step decideNotToWrite(State state, int index) {
            Copy copy = state.instances().get(index).copy();
            Row row = state.row();
            if (copy == null || !copy.trusted() || row == null || copy.value() == row.value()) {
                return null;
            }
            String name = "instance " + index + " replaces if " + row.value() + ", deciding on " + copy;
            if (stillStored(copy, row)) {
                return Step.violating(name, "instance " + index + " refused replace(" + row.value()
                        + ") on its copy " + copy + ", confirmed by the store holding " + row);
            }
            // decided again on the row read: the expected value, so written (a write step of its own)
            return Step.to(name, state);
        }

        // the failover: the row as the replica had it, the clock going on or standing where the replica's stood, and
        // a gap for every instance in place of what was not delivered
        private Step failover(State state, boolean clockStands) {
            List<Instance> instances = state.instances().stream()
                    .map(instance -> new Instance(instance.copy(), List.of(GAP), instance.clock(), instance.seen()))
                    .toList();
            return Step.to("failover" + (clockStands ? ", the clock standing" : ""),
                    state.withStore(state.replica(), clockStands ? state.replicaClock() : state.storeClock())
                            .withInstances(instances)
                            .withCounts(state.writes(), state.failovers() + 1, state.restarts()));
        }

        private boolean stillStored(Copy copy, @Nullable Row row) {
            return row != null && row.version() == copy.version()
                    && (check == Check.VERSION_ONLY || row.value() == copy.value());
        }
    }
}
