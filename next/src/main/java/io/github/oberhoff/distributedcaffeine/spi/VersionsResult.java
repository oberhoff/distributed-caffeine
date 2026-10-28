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
package io.github.oberhoff.distributedcaffeine.spi;

import java.time.Duration;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * What a read of versions found ({@link Store#readVersions}): per hash the version and the age of the value row's last
 * write; the ages of one read are measured at the same instant.
 *
 * @param versions the versions of the value rows found, by hash
 * @param ages     the ages of the same rows, by hash
 */
public record VersionsResult(Map<String, Long> versions, Map<String, Duration> ages) {

    public VersionsResult {
        requireNonNull(versions, "versions cannot be null");
        requireNonNull(ages, "ages cannot be null");
        if (!versions.keySet().equals(ages.keySet())) {
            throw new IllegalArgumentException("versions and ages must name the same rows");
        }
    }
}
