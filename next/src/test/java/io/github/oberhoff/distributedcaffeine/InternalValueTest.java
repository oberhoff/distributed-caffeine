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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class InternalValueTest {

    @Test
    void confirming_moves_the_epoch_forward_only() {
        InternalValue<String> value = new InternalValue<>("v", 5L, 1);

        value.confirm(3);
        value.confirm(2);

        assertThat(value.epoch()).isEqualTo(3);
        assertThat(value.version()).isEqualTo(5L);
        assertThat(value.value()).isEqualTo("v");
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // the null value is the point: it is rejected
    void versions_cannot_be_negative_and_values_must_be_present() {
        // 0 is the version of a value no store confirmed (InternalVersion.UNCONFIRMED)
        assertThat(new InternalValue<>("v", InternalVersion.UNCONFIRMED, 0).version()).isZero();
        assertThatThrownBy(() -> new InternalValue<>("v", -1L, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InternalValue<>(null, 1L, 0)).isInstanceOf(NullPointerException.class);
    }
}
