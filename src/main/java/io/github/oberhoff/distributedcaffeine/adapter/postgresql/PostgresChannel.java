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

import java.util.List;
import java.util.UUID;

import static java.lang.String.format;

/**
 * The notification channel of one scope, and how much of a publish fits into one notification.
 * <p>
 * The channel carries the scope so that a cache instance is only woken by writes it is concerned with, which is
 * what makes several caches sharing a table affordable. It is derived from the identifier rather than assembled
 * from its parts, because a channel name is an identifier: it is case-folded, and truncated at a length the server
 * decides, so two long scopes could end up sharing a channel without anything saying so. A digest always fits.
 */
final class PostgresChannel {

    // shorter than the documented limit, leaving room for the separators and for a limit that is configurable
    private static final int MAXIMUM_PAYLOAD_LENGTH = 7900;
    private static final String SEPARATOR = ",";
    private static final String PREFIX = "distributed_caffeine_";

    private PostgresChannel() {
        // utility class
    }

    static String channelOf(String identifier) {
        // 16 bytes of it, so the channel stays well inside any identifier length while collisions remain
        // something nobody will see - and a collision costs a read that matches nothing anyway, because the
        // read carries the discriminator
        return PREFIX + PostgresIdentifier.digestOf(identifier, 16);
    }

    // A channel of its own for one probe, so that nobody but the prober is woken by it. Random rather than derived
    // from the identifier, because two cache instances of one scope probing at once must not take each other's
    static String probeChannel() {
        return PREFIX + "probe_" + UUID.randomUUID().toString().replace("-", "");
    }

    // A publish larger than one payload becomes several notifications rather than one that is rejected: the server
    // refuses an over-long payload at the call, so this has to hold before anything is sent. Distinct payloads are
    // always delivered, so the parts cannot fold into each other the way identical ones would
    static List<String> payloadsOf(List<String> hashes) {
        List<String> payloads = new java.util.ArrayList<>();
        StringBuilder payload = new StringBuilder();
        for (String hash : hashes) {
            if (hash.contains(SEPARATOR)) {
                throw new IllegalArgumentException(format("hash cannot contain '%s', but was '%s'", SEPARATOR, hash));
            }
            if (!payload.isEmpty() && payload.length() + SEPARATOR.length() + hash.length() > MAXIMUM_PAYLOAD_LENGTH) {
                payloads.add(payload.toString());
                payload.setLength(0);
            }
            if (!payload.isEmpty()) {
                payload.append(SEPARATOR);
            }
            payload.append(hash);
        }
        if (!payload.isEmpty()) {
            payloads.add(payload.toString());
        }
        return payloads;
    }

    static List<String> hashesOf(String payload) {
        return List.of(payload.split(SEPARATOR));
    }
}
