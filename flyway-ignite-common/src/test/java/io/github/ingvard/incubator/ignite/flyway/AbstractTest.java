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

package io.github.ingvard.incubator.ignite.flyway;

import static io.github.ingvard.incubator.ignite.flyway.common.util.IgniteUtils.LOOPBACK;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteJdbcThinDataSource;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Abstract test with an embedded ignite node.
 */
public class AbstractTest {
    /**
     * Thin client port.
     */
    protected static final int THIN_CLIENT_PORT = 5363;

    /**
     * Flyway schema history name.
     */
    protected static final String FLYWAY_SCHEMA_HISTORY_NAME = "SQL_PUBLIC_flyway_schema_history";

    /**
     * Setups.
     */
    @BeforeEach
    void setUp() throws IgniteCheckedException {
        IgnitionEx.start(getIgniteConfiguration(), false);
    }

    /**
     * Clean ups.
     */
    @AfterEach
    void tearDown() throws IgniteCheckedException {
        IgnitionEx.stopAll(true, null);

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "", true);
    }

    /**
     * Thin client datasource.
     */
    protected DataSource datasource() throws SQLException {
        IgniteJdbcThinDataSource src = new IgniteJdbcThinDataSource();

        src.setAddresses(jdbcAddress());

        return src;
    }

    /**
     * Thin client configuration.
     */
    protected ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration().setAddresses(jdbcAddress());
    }


    /**
     * Returns JDBC address.
     */
    protected String jdbcAddress() {
        return LOOPBACK + ":" + THIN_CLIENT_PORT;
    }

    /**
     * Default ignite configuration.
     */
    protected IgniteConfiguration getIgniteConfiguration() {
        return new IgniteConfiguration()
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                        .setHost(LOOPBACK)
                        .setPort(THIN_CLIENT_PORT)
                );
    }
}
