/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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

import java.io.Serializable;
import java.util.Objects;

import static java.lang.String.format;

@SuppressWarnings("unused")
public class Value implements Serializable {

    private Integer id;
    private String name;
    transient String data;

    public Value() {
    }

    private Value(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public Value setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Value setName(String name) {
        this.name = name;
        return this;
    }

    public String getData() {
        return data;
    }

    public Value setData(String data) {
        this.data = data;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        Value that = (Value) obj;
        return Objects.equals(this.id, that.id)
                && Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id);
    }

    @Override
    public String toString() {
        return format("Value{id=%s, name='%s'}", id, name);
    }

    public static Value of(Integer id) {
        return new Value(id, "value");
    }

    public static Value of(Integer id, String name) {
        return new Value(id, name);
    }
}
