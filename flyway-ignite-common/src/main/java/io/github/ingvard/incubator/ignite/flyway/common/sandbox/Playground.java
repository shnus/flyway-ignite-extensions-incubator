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

package io.github.ingvard.incubator.ignite.flyway.common.sandbox;

import static io.github.ingvard.incubator.ignite.flyway.common.util.IgniteUtils.LOOPBACK;
import static io.github.ingvard.incubator.ignite.flyway.common.util.IgniteUtils.getIsolatedConfiguration;

import io.github.ingvard.incubator.ignite.flyway.common.util.IgniteUtils;
import java.io.Closeable;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 * The Playground serves as a container for the database that runs in isolation.
 * Its purpose is to apply migrations in either a dry run or diff mode.
 */
public class Playground implements Closeable {
    /**
     * Connector port.
     */
    private static final int CONNECTOR_PORT = 12333;

    /**
     * Client.
     */
    @Getter
    private IgniteClient client;

    /**
     * Embedded node.
     */
    private IgniteEx embeddedNode;

    /**
     * Starts a playground with an embedded ignite instance on random port from {@link Playground#CONNECTOR_PORT}.
     */
    @SneakyThrows
    public synchronized void play() {
        IgniteConfiguration cfg = getIsolatedConfiguration();

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setHost(LOOPBACK)
                .setPort(CONNECTOR_PORT)
        );

        embeddedNode = IgniteUtils.startIgnite(cfg);

        int port = embeddedNode.context().sqlListener().port();

        client = Ignition.startClient(new ClientConfiguration()
                .setAddresses(LOOPBACK + ":" + port));
    }

    /**
     * {@inheritDoc}
     */
    @Override public synchronized void close() {
        if (client != null) {
            client.close();
        }

        if (embeddedNode != null) {
            embeddedNode.close();
        }
    }
}
