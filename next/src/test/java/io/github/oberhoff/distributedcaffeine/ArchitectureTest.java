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
 * The boundary between the core and the adapters (VISION.md, "Coordinates, packaging and version"): an adapter
 * package uses nothing of the library but the SPI, so it can move into an artifact of its own later.
 */
@NullMarked
class ArchitectureTest {

    private static final Path SOURCES = Path.of("src/main/java/io/github/oberhoff/distributedcaffeine");

    @Test
    void adapters_use_only_the_spi() throws IOException {
        for (String adapter : List.of("postgresql", "mongodb", "jdbc")) {
            Path directory = SOURCES.resolve(adapter);
            if (!Files.isDirectory(directory)) {
                continue;
            }
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
}
