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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * How a name the adapter derives is kept inside what PostgreSQL allows an identifier to be.
 * <p>
 * The server truncates an identifier that is too long instead of refusing it, so two names that agree up to that
 * length silently become one name. What that costs depends on what the name is for, and it is never nothing: a
 * shared channel wakes cache instances for writes they have nothing to do with, and a shared index name makes
 * {@code CREATE INDEX IF NOT EXISTS} find an index that belongs to another table and leave this one without one.
 * A digest of the whole name carries what a truncation would drop, so names that differ stay different.
 */
final class PostgresIdentifier {

    // NAMEDATALEN - 1, the server's own limit. Every identifier the adapter derives is ASCII (the builder allows
    // nothing else), so what counts here is characters and bytes alike
    private static final int MAXIMUM_LENGTH = 63;
    private static final int DIGEST_BYTES = 8;

    private PostgresIdentifier() {
        // utility class
    }

    /**
     * The identifier itself while it fits, and otherwise as much of it as fits beside a digest of the whole -
     * which keeps a name legible where it can be and unique where it cannot.
     */
    static String limited(String identifier) {
        if (identifier.length() <= MAXIMUM_LENGTH) {
            return identifier;
        }
        String digest = "_" + digestOf(identifier, DIGEST_BYTES);
        return identifier.substring(0, MAXIMUM_LENGTH - digest.length()) + digest;
    }

    /**
     * The leading bytes of the SHA-256 of a value, as hexadecimal. Enough of them that a collision is something
     * nobody will see, and few enough that what carries the digest still fits.
     */
    static String digestOf(String value, int bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
