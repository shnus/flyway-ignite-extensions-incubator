/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.github.ingvard.incubator.ignite.flyway.commandline.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.flywaydb.core.api.logging.Log;

/**
 * Util class for log testing.
 */
public class TestableLogger implements Log {
    /**
     * Registry.
     */
    public static final Map<Class<?>, TestableLogger> registry = new ConcurrentHashMap<>();

    /**
     * Silent.
     */
    @Setter
    public static volatile boolean silent = false;

    /**
     * Logs.
     */
    @Getter
    private final List<String> logs = Collections.synchronizedList(new ArrayList<>());

    /**
     * Create logger.
     *
     * @param clazz Clazz.
     */
    public static TestableLogger getOrCreateLogger(Class<?> clazz) {
        TestableLogger testableLog = new TestableLogger();

        return registry.computeIfAbsent(clazz, ignore -> testableLog);
    }

    /**
     * Unregisters all loggers.
     */
    public static void reset() {
        registry.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean isDebugEnabled() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void debug(String msg) {
        if (!silent) {
            System.out.println(msg);
        }

        logs.add(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void info(String msg) {
        if (!silent) {
            System.out.println(msg);
        }

        logs.add(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void warn(String msg) {
        if (!silent) {
            System.out.println(msg);
        }

        logs.add(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void error(String msg) {
        if (!silent) {
            System.out.println(msg);
        }

        logs.add(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void error(String msg, Exception e) {
        if (!silent) {
            System.out.println(msg);
        }

        logs.add(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void notice(String msg) {
        if (!silent) {
            System.out.println(msg);
        }

        logs.add(msg);
    }
}
