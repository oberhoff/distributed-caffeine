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

import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The schema marker a store keeps with a cache's place (a table comment, a metadata document, a key): which layout
 * its rows have and which hash format identifies them, as {@code distributed-caffeine schema=1 hash=xxh3-128/1}. Pairs
 * of a key and a value follow the library's name, each value of letters, digits and {@code -./}; a later version adds
 * pairs, never changes the meaning of one.
 * <p>
 * The newer version wins: a store compares a place's marker with this version's ({@link #relationOf}). A place of an
 * {@link Relation#EARLIER earlier} version may be dropped and recreated by a store offering a reset; a place of a
 * {@link Relation#LATER later} version is never touched, so that two versions cannot drop each other's place in turns,
 * and has to be dropped by hand to go back. Versions are ordered by the schema, then by the revision of the hash
 * format (its number after the {@code /}).
 *
 * @author Andreas Oberhoff
 */
public final class StoreMarker {

    /**
     * The name every marker of this library begins with.
     */
    public static final String LIBRARY = "distributed-caffeine";

    /**
     * The layout of this version's rows.
     */
    public static final int SCHEMA = 1;

    /**
     * This version's marker, as a store writes it.
     */
    public static final String CURRENT = LIBRARY + " schema=" + SCHEMA + " hash=" + Store.HASH_FORMAT;

    private static final Pattern PAIR = Pattern.compile("([a-z]+)=([A-Za-z0-9./-]+)");
    // the marker without pairs that versions before the hash format wrote: "distributed-caffeine schema 1", or "1"
    private static final Pattern EARLY = Pattern.compile("(?:" + LIBRARY + " schema )?(\\d+)");

    private StoreMarker() {
    }

    /**
     * How a place's marker relates to this version's.
     */
    public enum Relation {

        /**
         * This version's marker: the place is used.
         */
        SAME,

        /**
         * A marker of an earlier version of this library: refused, or the place dropped and recreated by a reset.
         */
        EARLIER,

        /**
         * A marker of a later version of this library, or one it cannot order: refused, also by a reset, and dropped
         * by hand to go back.
         */
        LATER,

        /**
         * No marker of this library: the place is left to the store's own rules (created by hand, or foreign).
         */
        FOREIGN
    }

    /**
     * Returns how a place's marker relates to this version's.
     *
     * @param marker the place's marker, or {@code null} if it has none
     * @return the relation
     */
    public static Relation relationOf(@Nullable String marker) {
        if (marker == null) {
            return Relation.FOREIGN;
        }
        if (CURRENT.equals(marker)) {
            return Relation.SAME;
        }
        var early = EARLY.matcher(marker);
        if (early.matches()) {
            return order(Integer.parseInt(early.group(1)), -1);
        }
        if (!marker.startsWith(LIBRARY + " ")) {
            return Relation.FOREIGN;
        }
        Map<String, String> pairs = pairs(marker);
        if (pairs == null) {
            // the library's name, then what this version cannot read: written by a later one
            return Relation.LATER;
        }
        String schema = pairs.remove("schema");
        String hash = pairs.remove("hash");
        if (schema == null || hash == null || !schema.matches("\\d+") || !pairs.isEmpty()) {
            // a pair this version does not know: written by a later one
            return Relation.LATER;
        }
        Relation order = order(Integer.parseInt(schema), revision(hash));
        if (order == Relation.SAME) {
            // the same order, another algorithm: no version this one knows
            return Relation.LATER;
        }
        return order;
    }

    // by the schema, then by the hash format's revision (-1 for none)
    private static Relation order(int schema, int revision) {
        int bySchema = Integer.compare(schema, SCHEMA);
        if (bySchema != 0) {
            return bySchema < 0 ? Relation.EARLIER : Relation.LATER;
        }
        int byRevision = Integer.compare(revision, revision(Store.HASH_FORMAT));
        if (byRevision != 0) {
            return byRevision < 0 ? Relation.EARLIER : Relation.LATER;
        }
        return Relation.SAME;
    }

    // the number after the last '/', or a revision above every known one if there is none
    private static int revision(String hashFormat) {
        int slash = hashFormat.lastIndexOf('/');
        String number = slash < 0 ? "" : hashFormat.substring(slash + 1);
        return number.matches("\\d{1,9}") ? Integer.parseInt(number) : Integer.MAX_VALUE;
    }

    // the pairs after the library's name, or null if they cannot be read
    private static @Nullable Map<String, String> pairs(String marker) {
        String[] parts = marker.split(" ", -1);
        Map<String, String> pairs = new LinkedHashMap<>();
        for (int i = 1; i < parts.length; i++) {
            var pair = PAIR.matcher(parts[i]);
            if (!pair.matches() || pairs.put(pair.group(1), pair.group(2)) != null) {
                return null;
            }
        }
        return pairs;
    }
}
