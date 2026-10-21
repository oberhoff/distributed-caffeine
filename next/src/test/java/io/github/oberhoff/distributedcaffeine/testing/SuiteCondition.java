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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

/**
 * Runs a {@link Suite} only where its topology hosts it, or where the system properties ask for it; and a topology's
 * test class only if one of its suites runs, so that no store is started for nothing.
 */
public final class SuiteCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (context.getTestMethod().isPresent() || context.getTestClass().isEmpty()) {
            return ConditionEvaluationResult.enabled("not a class");
        }
        Class<?> type = context.getRequiredTestClass();
        Suite suite = type.getAnnotation(Suite.class);
        if (suite != null) {
            Topology topology = topology(context);
            return runs(topology, suite.value())
                    ? ConditionEvaluationResult.enabled(topology + " runs " + suite.value())
                    : ConditionEvaluationResult.disabled(topology + " does not run " + suite.value()
                    + " (its suites, -DallSuites, -Dsuites)");
        }
        Topology topology = declaredTopology(type);
        if (topology != null && Arrays.stream(Suite.Kind.values()).noneMatch(kind -> runs(topology, kind))) {
            return ConditionEvaluationResult.disabled("none of the suites of " + topology + " runs (-Dsuites)");
        }
        return ConditionEvaluationResult.enabled("runs");
    }

    // hosted by the topology, or general and asked for everywhere; and among -Dsuites, if given
    private static boolean runs(Topology topology, Suite.Kind kind) {
        String only = System.getProperty("suites", "");
        if (!only.isBlank() && Arrays.stream(only.split(",")).map(String::trim).noneMatch(kind.name()::equals)) {
            return false;
        }
        return topology.suites().contains(kind) || (kind.isGeneral() && Boolean.getBoolean("allSuites"));
    }

    // the topology a test class names in an annotation of its store (a member topology() of a Topology type)
    private static @Nullable Topology declaredTopology(Class<?> type) {
        if (!Topology.Tests.class.isAssignableFrom(type)) {
            return null;
        }
        for (Annotation annotation : type.getAnnotations()) {
            try {
                Method member = annotation.annotationType().getMethod("topology");
                if (Topology.class.isAssignableFrom(member.getReturnType())) {
                    member.setAccessible(true); // the store's annotation is package-private
                    return (Topology) member.invoke(annotation);
                }
            } catch (NoSuchMethodException e) {
                // another annotation
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }
        return null;
    }

    // the topology of the enclosing test class, whose instance exists already (one instance per class)
    private static Topology topology(ExtensionContext context) {
        for (Optional<ExtensionContext> parent = context.getParent(); parent.isPresent();
             parent = parent.get().getParent()) {
            Optional<Object> instance = parent.get().getTestInstance();
            if (instance.isPresent() && instance.get() instanceof Topology.Tests tests) {
                return tests.topology();
            }
        }
        throw new IllegalStateException(context.getRequiredTestClass() + " is a suite outside a topology");
    }
}
