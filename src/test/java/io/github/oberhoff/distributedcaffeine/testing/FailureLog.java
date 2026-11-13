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
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Records every failed test and container (a store that did not start fails its topology's class) in a file of the
 * test JVM's own under {@code target/test-failures}, one line each: the display names from the class down, the
 * topology's included ("MongoDB 9.0.4 (Single-Node Replica Set) › Store contract › contract7_…"), a tab, and the
 * failure on one line. Surefire's XML reports cannot serve: every topology of an integration test class runs in a JVM
 * of its own, and each rewrites the class's report, so only the last topology's results remain. The workflows turn
 * these lines into annotations and a job summary. Registered through {@code META-INF/services}.
 *
 * @author Andreas Oberhoff
 */
public final class FailureLog implements TestExecutionListener {

    private static final Path DIRECTORY = Path.of("target", "test-failures");
    private static final int MESSAGE_LENGTH = 400;

    private @Nullable TestPlan plan;

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        plan = testPlan;
    }

    @Override
    public void executionFinished(TestIdentifier identifier, TestExecutionResult result) {
        TestPlan current = plan;
        if (result.getStatus() != TestExecutionResult.Status.FAILED || current == null) {
            return;
        }
        String message = result.getThrowable().map(FailureLog::describe).orElse("failed");
        String line = path(current, identifier) + "\t" + message + System.lineSeparator();
        try {
            Files.createDirectories(DIRECTORY);
            Files.writeString(DIRECTORY.resolve(ProcessHandle.current().pid() + ".txt"), line,
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // the display names from the outermost class down, without the engine's
    private static String path(TestPlan plan, TestIdentifier identifier) {
        Deque<String> names = new ArrayDeque<>();
        for (TestIdentifier at = identifier; at != null; at = plan.getParent(at).orElse(null)) {
            if (plan.getParent(at).isPresent()) {
                names.addFirst(at.getDisplayName());
            }
        }
        return String.join(" › ", names);
    }

    // the failure's type and its message on one line (AssertJ puts the description and the values on lines of their
    // own), short enough for an annotation
    private static String describe(Throwable failure) {
        String message = failure.getMessage() == null ? "" : failure.getMessage();
        String line = (failure.getClass().getSimpleName() + ": " + message).replaceAll("\\s+", " ").strip();
        return line.length() <= MESSAGE_LENGTH ? line : line.substring(0, MESSAGE_LENGTH) + "…";
    }
}
