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
/**
 * The service provider interface between the core and a store adapter: a {@link
 * io.github.oberhoff.distributedcaffeine.spi.Store} for reads, writes and maintenance, and, for distribution, a
 * {@link io.github.oberhoff.distributedcaffeine.spi.ChangeFeed} announcing those writes, which a store implements
 * only if it can. The contract every implementation must fulfil is written out on {@link
 * io.github.oberhoff.distributedcaffeine.spi.Store} (storage, failures, settings) and {@link
 * io.github.oberhoff.distributedcaffeine.spi.ChangeFeed} (announcements, delivery, supervision), each point checked
 * by the contract tests of its name; {@link io.github.oberhoff.distributedcaffeine.spi.StoreException} and {@link
 * io.github.oberhoff.distributedcaffeine.spi.Subscription} hold the details of failures and resuming. The design
 * behind it: {@code next/VISION.md}, "SPI: a write is the message".
 */
@NullMarked
package io.github.oberhoff.distributedcaffeine.spi;

import org.jspecify.annotations.NullMarked;
