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

import io.github.oberhoff.distributedcaffeine.hasher.Hash;
import org.jspecify.annotations.NullMarked;

/**
 * A test key's hash computed without hash4j, whose byte-array VarHandles Lincheck's model checking cannot run: the
 * key's length and Java's string hash, distinct for the short keys of the tests.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
public final class TestHashes {

    private TestHashes() {
    }

    /**
     * Returns a test key's hash.
     *
     * @param key the key
     * @return the hash
     */
    public static Hash of(String key) {
        return new Hash(key.length(), key.hashCode());
    }
}
