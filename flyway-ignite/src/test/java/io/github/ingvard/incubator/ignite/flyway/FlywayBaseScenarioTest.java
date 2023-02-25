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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfoService;
import org.junit.jupiter.api.Test;

/**
 * Base scenario tests.
 */
public class FlywayBaseScenarioTest extends AbstractTest {
    /**
     * Should migrate base scenario migrations.
     */
    @Test
    public void shouldMigratePlaneSql() throws SQLException {
        Flyway fl = Flyway.configure()
                .locations("/db/scenario/base")
                .dataSource(datasource())
                .load();

        fl.migrate();

        assertThat(ignite.cacheNames()).containsExactlyInAnyOrder(
                FLYWAY_SCHEMA_HISTORY_NAME,
                "PetCache",
                "OwnerCache"
        );
    }

    /**
     * Should clean all tables.
     */
    @Test
    public void shouldClean() throws SQLException {
        shouldMigratePlaneSql();

        Flyway fl = Flyway.configure()
                .dataSource(datasource())
                .cleanDisabled(false)
                .load();

        fl.clean();

        assertThat(ignite.cacheNames()).isEmpty();
    }

    /**
     * Should return migration info.
     */
    @Test
    public void shouldInfo() throws SQLException {
        shouldMigratePlaneSql();

        Flyway fl = Flyway.configure()
                .dataSource(datasource())
                .load();

        MigrationInfoService info = fl.info();

        assertThat(info.getInfoResult().schemaVersion).isEqualTo("2");
        assertThat(info.all())
                .extracting("description")
                .containsExactlyInAnyOrder("first", "second");
    }
}
