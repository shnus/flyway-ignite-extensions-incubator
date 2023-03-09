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

package io.github.ingvard.incubator.ignite.flyway.commandline.command.snapshot;

import static io.github.ingvard.incubator.ignite.flyway.commandline.command.snapshot.SnapshotCommandExtension.SnapshotCommandFlag.UNKNOWN_TYPE_SUPPORTED;
import static io.github.ingvard.incubator.ignite.flyway.commandline.command.snapshot.SnapshotCommandExtension.SnapshotCommandProperty.SNAPSHOT_NAME;
import static io.github.ingvard.incubator.ignite.flyway.commandline.command.snapshot.SnapshotCommandExtension.SnapshotCommandProperty.SNAPSHOT_WORK_DIR;

import io.github.ingvard.incubator.ignite.flyway.commandline.command.CommandlineFlag;
import io.github.ingvard.incubator.ignite.flyway.commandline.command.CommandlineProperty;
import io.github.ingvard.incubator.ignite.flyway.commandline.config.FlywayIgniteConfigurationExtractor;
import io.github.ingvard.incubator.ignite.flyway.common.snapshot.Dumper;
import io.github.ingvard.incubator.ignite.flyway.common.snapshot.DumperConfiguration;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ignite.IgniteJdbcThinDataSource;
import org.apache.ignite.internal.util.typedef.F;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.output.OperationResult;
import org.flywaydb.core.extensibility.CommandExtension;
import org.flywaydb.core.internal.util.Pair;

/**
 * Ignite snapshot extension.
 */
public class SnapshotCommandExtension implements CommandExtension {
    /**
     * Snapshot commands.
     */
    public static final List<String> SNAPSHOT_COMMANDS = Arrays.asList("snapshot", "ss");

    /**
     * Snapshot command flags.
     */
    @Getter
    @RequiredArgsConstructor
    public enum SnapshotCommandFlag implements CommandlineFlag {
        /**
         * Unknown type supported.
         */
        UNKNOWN_TYPE_SUPPORTED(List.of("unsafe-type", "ut")),

        /**
         * Full snapshot.
         */
        FULL_SNAPSHOT(List.of("full"));

        /**
         * Aliases.
         */
        private final List<String> aliases;
    }

    /**
     * Snapshot command properties.
     */
    @Getter
    @RequiredArgsConstructor
    public enum SnapshotCommandProperty implements CommandlineProperty {
        /**
         * Snapshot name.
         */
        SNAPSHOT_NAME("snapshot.name"),
        /**
         * Snapshot work directory.
         */
        SNAPSHOT_WORK_DIR("snapshot.dir");

        /**
         * Property name.
         */
        private final String name;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean handlesCommand(String cmd) {
        return SNAPSHOT_COMMANDS.contains(cmd);
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean handlesParameter(String param) {
        return Arrays.stream(SnapshotCommandFlag.values()).anyMatch(e -> e.match(param));
    }

    /**
     * {@inheritDoc}
     */
    @Override public OperationResult handle(String cmd, Configuration cfg, List<String> flags) throws FlywayException {
        DataSource dataSrc = cfg.getDataSource();

        if (!(dataSrc instanceof IgniteJdbcThinDataSource)) {
            throw new IllegalStateException(String.format("Wrong datasource [actual=%s, expected=%s]",
                    dataSrc.getClass().getCanonicalName(),
                    IgniteJdbcThinDataSource.class.getCanonicalName()
            ));
        }

        IgniteJdbcThinDataSource igniteDataSrc = (IgniteJdbcThinDataSource) dataSrc;

        String username = igniteDataSrc.getUsername();
        String pwd = igniteDataSrc.getPassword();

        if (!F.isEmpty(username) && !F.isEmpty(pwd)) {
            throw new UnsupportedOperationException(
                    "Secure connection hasn't supported yet. Please create an issue request:"
                            + " https://github.com/ingvard/flyway-ignite-extensions-incubator");
        }

        DumperConfiguration dpCfg = Dumper.configure()
                .setAddresses(igniteDataSrc.getAddresses());

        if (flags.stream().anyMatch(UNKNOWN_TYPE_SUPPORTED::match)) {
            dpCfg.unknownTypeSupport(true);
        }

        String snapshotDir = FlywayIgniteConfigurationExtractor.getProperty(SNAPSHOT_WORK_DIR);

        if (!F.isEmpty(snapshotDir)) {
            dpCfg.location(snapshotDir);
        }

        Dumper dp = dpCfg.load();

        String snapshotName = FlywayIgniteConfigurationExtractor.getProperty(SNAPSHOT_NAME);

        if (!F.isEmpty(snapshotName)) {
            return dp.createFullSchemaSnapshot(snapshotName);
        } else {
            return dp.createFullSchemaSnapshot();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public List<Pair<String, String>> getUsage() {
        return List.of(Pair.of(String.join(", ", SNAPSHOT_COMMANDS), "Make a schema snapshot"));
    }
}
