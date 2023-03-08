/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.ingvard.incubator.ignite.flyway.common.util;

import io.github.ingvard.incubator.ignite.flyway.common.network.IsolatedCommunicationSpi;
import io.github.ingvard.incubator.ignite.flyway.common.network.IsolatedDiscoverySpi;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;

/**
 * Ignite base utils.
 */
public class IgniteUtils {
    /**
     * Loopback.
     */
    public static final String LOOPBACK = "0.0.0.0";

    /**
     * Starts an isolated ignite instance.
     */
    public static IgniteEx startIsolatedIgnite() throws IgniteCheckedException {
        return (IgniteEx) IgnitionEx.start(getIsolatedConfiguration(), false);
    }

    /**
     * Starts an ignite instance.
     *
     * @param cfg Config.
     */
    public static IgniteEx startIgnite(IgniteConfiguration cfg) throws IgniteCheckedException {
        return (IgniteEx) IgnitionEx.start(cfg, false);
    }

    /**
     * Default ignite configuration.
     */
    public static IgniteConfiguration getIsolatedConfiguration() {
        return new IgniteConfiguration()
                .setIgniteInstanceName("isolated-" + UUID.randomUUID())
                .setDiscoverySpi(new IsolatedDiscoverySpi())
                .setCommunicationSpi(new IsolatedCommunicationSpi())
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                        .setHost(LOOPBACK)
                );
    }
}
