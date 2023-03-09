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

package io.github.ingvard.incubator.ignite.flyway.commandline;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ingvard.incubator.ignite.flyway.commandline.utils.TestableLogger;
import java.lang.reflect.Constructor;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.assertj.core.api.ListAssert;
import org.flywaydb.commandline.Main;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.extensibility.CommandExtension;
import org.flywaydb.core.internal.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Pre-configuration of commandline test.
 */
public abstract class CommandlineAbstractTest {
    /**
     * Base sources of logs.
     */
    @RequiredArgsConstructor
    public enum LogModule {
        /**
         * Main console.
         */
        MAIN_CONSOLE(Main.class);

        /**
         * Clazz.
         */
        @Getter
        private final Class<?> clazz;
    }

    /**
     * Initialize test env.
     */
    @BeforeEach
    void setUp() {
        LogFactory.setLogCreator(TestableLogger::getOrCreateLogger);
    }

    /**
     * Cleanup test env.
     */
    @AfterEach
    void tearDown() {
        LogFactory.setLogCreator(null);

        TestableLogger.reset();
    }

    /**
     * Runs cli without arguments.
     */
    protected void runCliWithoutArguments() {
        Main.main(new String[] {});
    }

    /**
     * Log assertion.
     *
     * @param logModule Logger module.
     */
    protected ListAssert<String> assertThatLog(LogModule logModule) {
        List<String> logs = TestableLogger.getOrCreateLogger(Main.class).getLogs();

        return assertThat(logs);
    }

    @SneakyThrows
    protected List<Pair<String, String>> usageMessage(Class<? extends CommandExtension> clazz) {
        Constructor<? extends CommandExtension> constructor = clazz.getConstructor();

        CommandExtension cmdExtension = constructor.newInstance(new Object[] {});

        return cmdExtension.getUsage();
    }
}
