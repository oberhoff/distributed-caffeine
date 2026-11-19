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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The boundaries between the core, the adapters and the integrations (VISION.md, "Coordinates, packaging and
 * version", "Integrations: one subpackage per framework"): an adapter package uses nothing of the library but the SPI,
 * an integration package nothing but the public API, so either can move into an artifact of its own later.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class ArchitectureTest {

    private static final Path SOURCES = Path.of("src/main/java/io/github/oberhoff/distributedcaffeine");

    @Test
    void adapters_use_only_the_spi() throws IOException {
        List<Path> adapters;
        try (Stream<Path> directories = Files.list(SOURCES.resolve("adapter"))) {
            adapters = directories.filter(Files::isDirectory).toList();
        }
        assertThat(adapters).as("adapter packages").isNotEmpty();
        for (Path directory : adapters) {
            String adapter = "adapter\\." + directory.getFileName();
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : files.toList()) {
                    for (String line : Files.readAllLines(file)) {
                        if (line.startsWith("import") && line.contains("io.github.oberhoff.distributedcaffeine.")) {
                            assertThat(line).as(file.getFileName() + " imports outside the SPI")
                                    .matches("import (static )?io\\.github\\.oberhoff\\.distributedcaffeine\\.(spi|"
                                            + adapter + ")\\..*");
                        }
                    }
                }
            }
        }
    }

    @Test
    void integrations_use_only_the_public_api() throws IOException {
        List<Path> integrations;
        try (Stream<Path> directories = Files.list(SOURCES.resolve("integration"))) {
            integrations = directories.filter(Files::isDirectory).toList();
        }
        assertThat(integrations).as("integration packages").isNotEmpty();
        for (Path directory : integrations) {
            String integration = "integration\\." + directory.getFileName();
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : files.toList()) {
                    for (String line : Files.readAllLines(file)) {
                        if (line.startsWith("import") && line.contains("io.github.oberhoff.distributedcaffeine.")) {
                            // the root package without its Internal classes, hashers, serializers, its own package
                            assertThat(line).as(file.getFileName() + " imports outside the public API")
                                    .matches("import (static )?io\\.github\\.oberhoff\\.distributedcaffeine\\."
                                            + "((?!Internal)[A-Z]\\w*|(hasher|serializer|" + integration
                                            + ")\\.).*");
                        }
                    }
                }
            }
        }
    }
}
