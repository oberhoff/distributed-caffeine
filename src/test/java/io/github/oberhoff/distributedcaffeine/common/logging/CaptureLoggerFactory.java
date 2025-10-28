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
package io.github.oberhoff.distributedcaffeine.common.logging;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CaptureLoggerFactory implements ILoggerFactory {

    private final ConcurrentMap<String, CaptureLogger> loggerMap;

    public CaptureLoggerFactory() {
        loggerMap = new ConcurrentHashMap<>();
    }

    @Override
    public Logger getLogger(String name) {
        return loggerMap.computeIfAbsent(name, k -> new CaptureLogger(name));
    }

    public static CaptureLogger getCaptureLogger(Class<?> clazz) {
        return getCaptureLogger(clazz.getName());
    }

    public static CaptureLogger getCaptureLogger(String name) {
        return (CaptureLogger) LoggerFactory.getLogger(name);
    }
}