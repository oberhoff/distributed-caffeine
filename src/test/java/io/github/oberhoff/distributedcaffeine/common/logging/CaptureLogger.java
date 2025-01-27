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

import org.slf4j.Marker;
import org.slf4j.event.DefaultLoggingEvent;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.helpers.AbstractLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.nonNull;

public class CaptureLogger extends AbstractLogger {

    private boolean capturing;
    private final List<LoggingEvent> loggingEvents;

    public CaptureLogger(String name) {
        this.name = name;
        this.capturing = false;
        this.loggingEvents = new ArrayList<>();
    }

    public void startCapturing() {
        if (capturing) {
            throw new IllegalStateException("Already capturing");
        }
        this.capturing = true;
        this.loggingEvents.clear();
    }

    public void stopCapturing() {
        if (!capturing) {
            throw new IllegalStateException("Not capturing");
        }
        this.capturing = false;
        this.loggingEvents.clear();
    }

    public List<LoggingEvent> getLoggingEvents() {
        if (!capturing) {
            throw new IllegalStateException("Not capturing");
        }
        return Collections.unmodifiableList(loggingEvents);
    }

    @Override
    protected void handleNormalizedLoggingCall(Level level, Marker marker, String messagePattern, Object[] arguments,
                                               Throwable throwable) {
        if (capturing) {
            DefaultLoggingEvent defaultLoggingEvent = new DefaultLoggingEvent(level, this);
            defaultLoggingEvent.setMessage(messagePattern);
            defaultLoggingEvent.setThrowable(throwable);
            loggingEvents.add(defaultLoggingEvent);
        } else {
            if (level.toInt() >= Level.INFO.toInt()) {
                System.err.printf("%s [%s] %s - %s%n", level, Thread.currentThread().getName(), name, messagePattern);
                if (nonNull(throwable)) {
                    throwable.printStackTrace(System.err);
                }
            }
        }
    }

    @Override
    public boolean isTraceEnabled() {
        return true;
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return isWarnEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return isErrorEnabled();
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return getClass().getName();
    }
}
