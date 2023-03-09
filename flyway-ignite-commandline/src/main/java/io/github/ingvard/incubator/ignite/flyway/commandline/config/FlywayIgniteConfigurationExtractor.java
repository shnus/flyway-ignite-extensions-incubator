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

package io.github.ingvard.incubator.ignite.flyway.commandline.config;

import io.github.ingvard.incubator.ignite.flyway.commandline.command.CommandlineProperty;
import io.github.ingvard.incubator.ignite.flyway.commandline.command.snapshot.SnapshotCommandExtension;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.flywaydb.core.api.configuration.ClassicConfiguration;
import org.flywaydb.core.extensibility.ConfigurationExtension;

/**
 * Regrettably, the Flyway configuration system relies on a Java plugin system {@link ClassicConfiguration#configure(Map)},
 * which means that instead of using dependency injection, one must use a static object to extract configurations.
 */
public class FlywayIgniteConfigurationExtractor implements ConfigurationExtension {
    /**
     * Default property prefix.
     */
    private static final String DEFAULT_PROPERTY_PREFIX = "flyway.";

    /**
     * Properties.
     */
    private static final Map<String, String> PROPS = new ConcurrentHashMap<>();

    /**
     * Gets property value or null if it doesn't exist.
     *
     * @param prop Property.
     */
    public static String getProperty(CommandlineProperty prop) {
        return PROPS.get(prop.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override public void extractParametersFromConfiguration(Map<String, String> cfg) {
        for (SnapshotCommandExtension.SnapshotCommandProperty prop : SnapshotCommandExtension.SnapshotCommandProperty.values()) {
            String propNameWithPrefix = DEFAULT_PROPERTY_PREFIX + prop.getName();

            if (cfg.containsKey(propNameWithPrefix)) {
                PROPS.put(prop.getName(), cfg.remove(propNameWithPrefix));
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public String getConfigurationParameterFromEnvironmentVariable(String environmentVariable) {
        return null;
    }
}
