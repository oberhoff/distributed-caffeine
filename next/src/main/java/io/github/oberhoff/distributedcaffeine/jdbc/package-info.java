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
 * The store adapters for MariaDB ({@link io.github.oberhoff.distributedcaffeine.jdbc.MariaDbAdapter}) and MySQL
 * ({@link io.github.oberhoff.distributedcaffeine.jdbc.MySqlAdapter}) through JDBC: reads, conditional writes and
 * maintenance in SQL, without a change feed (the cache polls the changes).
 */
@NullMarked
package io.github.oberhoff.distributedcaffeine.jdbc;

import org.jspecify.annotations.NullMarked;
