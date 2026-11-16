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
package io.github.oberhoff.distributedcaffeine.testing;

import org.jetbrains.kotlinx.lincheck.CTestStructure;
import org.jetbrains.kotlinx.lincheck.execution.RandomExecutionGenerator;
import org.jetbrains.lincheck.datastructures.CTestConfiguration;
import org.jetbrains.lincheck.datastructures.ModelCheckingOptions;
import org.jetbrains.lincheck.datastructures.ParameterGenerator;
import org.jetbrains.lincheck.datastructures.RandomProvider;

/**
 * Lincheck's random scenarios, split into shards that run in parallel jobs: with {@code -Dlincheck.shard=k/n}, a model
 * checking run checks only the k-th of n consecutive slices of its scenarios. Lincheck derives every scenario from a
 * fixed seed (3.7: {@code RandomProvider}, seed 0), so scenario i is the same in every run, and checks each with a
 * strategy of its own; the slices together therefore check exactly the scenarios of an unsharded run. Without the
 * property the one shard is the whole run.
 * <p>
 * A shard generates the scenarios before its slice and drops them, as the parameter generators draw from random
 * sources of their own, which every scenario advances: only generating them leaves those where an unsharded run has
 * them at the slice's first scenario. Generating is cheap; checking costs the minutes.
 *
 * @author Andreas Oberhoff
 */
public final class ShardedScenarios extends RandomExecutionGenerator {

    // Lincheck creates the generator through reflection, with a fixed constructor, inside check(): the scenarios to
    // drop are handed over in this field, set by options() right before (a test JVM checks one class at a time)
    private static volatile int skipped;

    /**
     * Called by Lincheck through reflection.
     *
     * @param testConfiguration the test's configuration
     * @param testStructure     the test's operations and parameter generators
     * @param randomProvider    the source of the random generators
     */
    public ShardedScenarios(CTestConfiguration testConfiguration, CTestStructure testStructure,
                            RandomProvider randomProvider) {
        super(testConfiguration, testStructure, randomProvider);
        for (int i = 0; i < skipped; i++) {
            nextExecution();
            // as Lincheck's iteration loop does after each scenario (LinChecker.checkImpl)
            testStructure.parameterGenerators.forEach(ParameterGenerator::reset);
        }
    }

    /**
     * Restricts the options to this JVM's shard of the given scenarios.
     *
     * @param options    the model checking options
     * @param iterations the scenarios of an unsharded run
     * @return the options, checking the shard's scenarios only
     */
    public static ModelCheckingOptions options(ModelCheckingOptions options, int iterations) {
        return options(options, iterations, System.getProperty("lincheck.shard", "1/1"));
    }

    /**
     * Restricts the options to the given shard of the given scenarios.
     *
     * @param options    the model checking options
     * @param iterations the scenarios of an unsharded run
     * @param shard      the shard, k/n for the k-th of n
     * @return the options, checking the shard's scenarios only
     */
    public static ModelCheckingOptions options(ModelCheckingOptions options, int iterations, String shard) {
        if (!shard.matches("[1-9][0-9]*/[1-9][0-9]*")) {
            throw new IllegalArgumentException("lincheck.shard must be k/n with 1 <= k <= n: " + shard);
        }
        String[] parts = shard.split("/", -1);
        int index = Integer.parseInt(parts[0]);
        int count = Integer.parseInt(parts[1]);
        if (index > count) {
            throw new IllegalArgumentException("lincheck.shard must be k/n with 1 <= k <= n: " + shard);
        }
        int first = iterations * (index - 1) / count;
        int end = iterations * index / count;
        skipped = first;
        return options.iterations(end - first).executionGenerator(ShardedScenarios.class);
    }
}
