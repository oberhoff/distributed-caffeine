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

import io.github.oberhoff.distributedcaffeine.InternalLinearizabilityInvalidateAllTest;
import io.github.oberhoff.distributedcaffeine.InternalLinearizabilityTest;
import io.github.oberhoff.distributedcaffeine.InternalLinearizabilityTwoInstancesTest;
import org.jetbrains.kotlinx.lincheck.CTestStructure;
import org.jetbrains.lincheck.datastructures.CTestConfiguration;
import org.jetbrains.lincheck.datastructures.ModelCheckingOptions;
import org.jetbrains.lincheck.datastructures.ParameterGenerator;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shards of a model checking run check exactly its scenarios, in its order: the property behind splitting the
 * linearizability tests over parallel jobs, and what a Lincheck upgrade changing its scenario generation breaks.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class ShardedScenariosTest {

    private static final int ITERATIONS = 10;

    @ParameterizedTest
    @ValueSource(classes = {InternalLinearizabilityTest.class, InternalLinearizabilityInvalidateAllTest.class,
            InternalLinearizabilityTwoInstancesTest.class})
    void the_shards_together_are_the_unsharded_run(Class<?> testClass) {
        List<String> unsharded = scenarios(testClass, "1/1");
        assertThat(unsharded).hasSize(ITERATIONS).doesNotHaveDuplicates();
        for (int count = 2; count <= 4; count++) {
            List<String> joined = new ArrayList<>();
            for (int index = 1; index <= count; index++) {
                joined.addAll(scenarios(testClass, index + "/" + count));
            }
            assertThat(joined).as("%d shards", count).isEqualTo(unsharded);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "1", "0/3", "4/3", "1/0", "a/b", "1/3/1"})
    void a_malformed_shard_is_refused(String shard) {
        ModelCheckingOptions options = new ModelCheckingOptions();
        assertThatThrownBy(() -> ShardedScenarios.options(options, ITERATIONS, shard))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // as Lincheck's iteration loop draws them (LinChecker.checkImpl), from a structure of its own per run
    private static List<String> scenarios(Class<?> testClass, String shard) {
        CTestConfiguration configuration = ShardedScenarios.options(new ModelCheckingOptions(), ITERATIONS, shard)
                .threads(3).actorsPerThread(3).createTestConfigurations(testClass);
        CTestStructure structure = CTestStructure.getFromTestClass(testClass);
        ShardedScenarios generator = new ShardedScenarios(configuration, structure, structure.randomProvider);
        List<String> scenarios = new ArrayList<>();
        for (int i = 0; i < configuration.getIterations(); i++) {
            scenarios.add(generator.nextExecution().toString());
            structure.parameterGenerators.forEach(ParameterGenerator::reset);
        }
        return scenarios;
    }
}
