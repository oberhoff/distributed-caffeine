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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The texts the README shares with the Javadoc, kept word for word: the opening on the Javadoc's start page
 * ({@code overview.html}), and the introduction's model in the class Javadoc of {@link DistributedCaffeine}. Compared
 * as plain text: markup, links and the README's references to its own sections (which have no counterpart in the
 * Javadoc) left out. A change to one of them fails here until the other follows.
 *
 * @author Andreas Oberhoff
 */
class DocumentationTest {

    private static final Path README = Path.of("README.md");
    private static final Path OVERVIEW = Path.of("src/main/javadoc/overview.html");
    private static final Path BUILDER =
            Path.of("src/main/java/io/github/oberhoff/distributedcaffeine/DistributedCaffeine.java");

    @Test
    void the_javadoc_start_page_opens_as_the_readme_does() throws IOException {
        String readme = between(Files.readString(README), "# Distributed Caffeine\n", "## Table of contents");
        String overview = between(Files.readString(OVERVIEW), "<body>", "</ul>");
        assertThat(plain(overview)).isEqualTo(plainMarkdown(readme));
    }

    @Test
    void the_builder_describes_the_two_levels_as_the_readme_does() throws IOException {
        String start = "The two levels split the work:";
        String readme = between(Files.readString(README), start, "With distribution, a change made");
        String builder = between(Files.readString(BUILDER), start, "<pre>");
        assertThat(plain(builder)).isEqualTo(plainMarkdown(readme));
    }

    // the text from the start (included) to the end (excluded)
    private static String between(String text, String start, String end) {
        int from = text.indexOf(start);
        assertThat(from).as("found: %s", start).isNotNegative();
        int to = text.indexOf(end, from);
        assertThat(to).as("found: %s", end).isPositive();
        return text.substring(from, to);
    }

    // Markdown as plain text: no headings, bullets, emphasis or code marks, links as their text, and the references
    // to the README's own sections ("(see [Store](#store))") left out
    private static String plainMarkdown(String markdown) {
        // bullets by their line starts, then one space between words, so that a reference has exactly one before it
        String text = collapsed(markdown.replaceFirst("^# .*\n", "").replaceAll("(?m)^[ \\t]*+\\* ", " "));
        text = text.replaceAll(" \\(see \\[[^\\[\\]]*+]\\(#[^()]*+\\)\\)", "")
                .replaceAll("\\[([^\\[\\]]*+)]\\([^()]*+\\)", "$1")
                .replace("**", "")
                .replace("`", "");
        return collapsed(text);
    }

    // HTML or Javadoc as plain text: no comment stars or tags, inline tags as the text they show
    private static String plain(String html) {
        String text = html.replaceAll("(?m)^[ \\t]*+\\* ?", " ");
        text = replace(text, Pattern.compile("\\{@linkplain \\S++ ([^}]*+)}"), match -> match.group(1));
        // a member as its name, a type as its simple name
        text = replace(text, Pattern.compile("\\{@link (?:[^#}\\s]*+#)?([^}\\s]*+)}"),
                match -> match.group(1).substring(match.group(1).lastIndexOf('.') + 1));
        text = replace(text, Pattern.compile("\\{@code ([^}]*+)}"), match -> match.group(1));
        text = text.replaceAll("<[^>]*+>", "").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">");
        return collapsed(text);
    }

    private static String replace(String text, Pattern pattern, Function<MatchResult, String> replacement) {
        return pattern.matcher(text).replaceAll(match -> Matcher.quoteReplacement(replacement.apply(match)));
    }

    private static String collapsed(String text) {
        return text.replaceAll("\\s+", " ").trim();
    }
}
