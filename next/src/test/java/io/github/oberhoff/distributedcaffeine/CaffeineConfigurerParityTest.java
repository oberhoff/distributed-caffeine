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

import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every public configuration method of Caffeine's builder has its counterpart in {@link CaffeineConfigurer}, with
 * the same name and parameter types, returning the configurer. A method Caffeine adds fails this test until it is
 * followed.
 */
class CaffeineConfigurerParityTest {

    @Test
    void every_configuration_method_of_caffeine_has_its_counterpart() {
        Set<String> caffeine = signatures(Caffeine.class);
        Set<String> configurer = signatures(CaffeineConfigurer.class);

        assertThat(configurer).containsExactlyInAnyOrderElementsOf(caffeine);
        assertThat(caffeine).hasSize(21);
    }

    @Test
    void every_counterpart_returns_the_configurer() {
        List<Method> methods = configurationMethods(CaffeineConfigurer.class);

        assertThat(methods).isNotEmpty().allMatch(method -> method.getReturnType() == CaffeineConfigurer.class);
    }

    private static Set<String> signatures(Class<?> type) {
        return configurationMethods(type).stream()
                .map(method -> method.getName() + Arrays.stream(method.getParameterTypes()).map(Class::getName)
                        .collect(Collectors.joining(",", "(", ")")))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    // public instance methods declared by the class itself, without build methods and Object's methods
    private static List<Method> configurationMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isSynthetic() && !method.isBridge())
                .filter(method -> !method.getName().startsWith("build"))
                .filter(method -> !Set.of("toString", "equals", "hashCode").contains(method.getName()))
                .toList();
    }
}
