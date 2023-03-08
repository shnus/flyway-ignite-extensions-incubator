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

package io.github.ingvard.incubator.ignite.flyway.common.snapshot;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.github.ingvard.incubator.ignite.flyway.AbstractTest;
import java.sql.SQLException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.Test;

/**
 * Dumper tests.
 */
class DumperTest extends AbstractTest {
    /**
     * Full schema dump.
     */
    @Test
    public void fullSchemaDump() throws SQLException {
        Flyway fl = Flyway.configure()
                .locations("/db/scenario/snapshot/full")
                .dataSource(datasource())
                .load();

        MigrateResult migrate = fl.migrate();

        assertThat(migrate.success).isTrue();


        Dumper dp = Dumper.configure()
                .setAddresses(jdbcAddress())
                .load();

        SnapshotOperation result = dp.createFullSchemaSnapshot("test-snapshot-name");

        assertThat(result.isSuccess()).isTrue();
    }

}