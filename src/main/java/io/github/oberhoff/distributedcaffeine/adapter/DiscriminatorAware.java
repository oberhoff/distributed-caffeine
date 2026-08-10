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
package io.github.oberhoff.distributedcaffeine.adapter;

import org.jspecify.annotations.NullMarked;

/**
 * Interface representing objects that are aware of a discriminator.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
public interface DiscriminatorAware {

    /**
     * Sets the discriminator for this object.
     * <p>
     * <b>Note:</b> A discriminator selects which caches using the same relation in an underlying store belong
     * together, so it is never {@code null} - caches not choosing one of their own use
     * {@link Repository#DEFAULT_DISCRIMINATOR}.
     *
     * @param discriminator the discriminator to be set
     */
    void setDiscriminator(String discriminator);
}
