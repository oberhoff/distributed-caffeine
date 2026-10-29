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
package io.github.oberhoff.distributedcaffeine.redis;

import org.jspecify.annotations.Nullable;

import java.io.Serial;
import java.util.List;

import static java.util.Objects.requireNonNull;

// A command that failed, classified by the binding in the driver-neutral terms the store needs to decide whether a
// write may have applied and whether to try again (the store turns it into a StoreException)
final class RedisCommandException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    enum Kind {
        // no answer within the timeout: the command may have run (a write's outcome unknown); the connection was
        // dropped, as a silent one would fail every later command too
        TIMEOUT,
        // the connection broke or could not be opened: a command already sent may have run
        CONNECTION,
        // refused before running, for now: a replica (READONLY), a server loading or busy, a cluster in transition
        // (TRYAGAIN, CLUSTERDOWN, MASTERDOWN), a slot owned by another node (MOVED, ASK: where the client library
        // does not follow the redirect itself); nothing applied, worth another attempt
        UNAVAILABLE,
        // refused for lack of memory (OOM, maxmemory with noeviction): nothing applied, another attempt fails too
        OUT_OF_MEMORY,
        // a script failed while running: Redis does not roll back what it wrote before the error, so a write's
        // outcome is unknown; the library's scripts check before they write, so this is a bug
        SCRIPT,
        // anything else the server refused (a wrong argument, no permission): nothing applied
        REFUSED
    }

    // server errors meaning "not now, nothing ran": a replica, a server loading or busy, a cluster in transition, a
    // slot elsewhere
    private static final List<String> UNAVAILABLE_CODES = List.of("READONLY", "LOADING", "BUSY", "TRYAGAIN",
            "MASTERDOWN", "CLUSTERDOWN", "NOREPLICAS", "MOVED", "ASK");

    private final Kind kind;

    RedisCommandException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = requireNonNull(kind, "kind cannot be null");
    }

    Kind kind() {
        return kind;
    }

    // whether the command may have run: not where the server refused it before running it (REFUSED, UNAVAILABLE: a
    // replica refusing SCRIPT LOAD, as Kvrocks's does), so that something else may be tried in its place
    static boolean mayHaveRun(RedisCommandException e) {
        return e.kind() != Kind.REFUSED && e.kind() != Kind.UNAVAILABLE;
    }

    // a command the server answered with an error (the connection is fine), classified by the error's code, the same
    // for every client library
    static RedisCommandException answered(String what, @Nullable String error, Throwable cause) {
        String text = error == null ? "" : error;
        String message = what + " failed: " + text;
        if (UNAVAILABLE_CODES.stream().anyMatch(code -> hasCode(text, code))) {
            return new RedisCommandException(Kind.UNAVAILABLE, message, cause);
        }
        if (hasCode(text, "OOM")) {
            return new RedisCommandException(Kind.OUT_OF_MEMORY, message, cause);
        }
        // "ERR user_script:…" from Redis 7 and Valkey, "ERR Error running script …" before, "ERR running script …"
        // from Kvrocks
        if (text.contains("user_script") || text.contains("Error running script")
                || text.startsWith("ERR running script")) {
            return new RedisCommandException(Kind.SCRIPT, message, cause);
        }
        return new RedisCommandException(Kind.REFUSED, message, cause);
    }

    // whether the server's error carries the code: at its start ("READONLY You can't write …"), or, from a command a
    // script called, after the script's own error on Redis before 7 ("ERR Error running script (call to f_…):
    // @user_script:1: @user_script: 1: -READONLY You can't write …"). Such a command failed at the script's first
    // write (Redis checks memory and the role only there), so nothing of the script applied either way
    static boolean hasCode(String error, String code) {
        return error.startsWith(code + " ") || error.contains(" -" + code + " ");
    }
}
