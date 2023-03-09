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

import io.github.ingvard.incubator.ignite.flyway.common.sandbox.Playground;
import io.github.ingvard.incubator.ignite.flyway.common.sql.SqlTypeMapping;
import io.github.ingvard.incubator.ignite.flyway.common.sql.generator.SqlGenerator;
import io.github.ingvard.incubator.ignite.flyway.common.sql.mapper.IntermediateSchemaMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * Dumper configuration.
 */
@Getter
public class DumperConfiguration {
    /**
     * Default location.
     */
    public static final String DEFAULT_LOCATION = Paths.get(System.getProperty("user.dir"), "flyway-snapshots").toString();

    /**
     * Default snapshot name.
     */
    public static final String DEFAULT_SNAPSHOT_NAME = "snapshot.sql";

    /**
     * Location.
     */
    private String location = DEFAULT_LOCATION;

    /**
     * Snapshot name.
     */
    private String snapshotName = DEFAULT_SNAPSHOT_NAME;

    /**
     * Unknown type support.
     */
    private boolean unknownTypeSupport = false;

    /** Addrs. */
    private String[] addrs;

    /**
     * Sets snapshot work dir.
     *
     * @param location Location.
     */
    public DumperConfiguration location(String location) {
        this.location = location;

        return this;
    }

    /**
     * If {@link DumperConfiguration#isUnknownTypeSupport()} is {@code true}, it disables type checks for unknown values
     * and sets {@link SqlTypeMapping#OTHER} for them.
     *
     * @param unknownTypeSupport Unknown type support.
     */
    public DumperConfiguration unknownTypeSupport(boolean unknownTypeSupport) {
        this.unknownTypeSupport = unknownTypeSupport;

        return this;
    }

    /**
     * Sets a snapshot name prefix.
     *
     * @param snapshotName Snapshot name.
     */
    public DumperConfiguration snapshotName(String snapshotName) {
        this.snapshotName = snapshotName;

        return this;
    }

    /**
     * Sets ignite JDBC addresses.
     *
     * @param addrs Addresses.
     */
    public DumperConfiguration setAddresses(String... addrs) {
        if (addrs != null) {
            this.addrs = Arrays.copyOf(addrs, addrs.length);
        }

        return this;
    }

    /**
     * Create a Dumper instance.
     */
    @SneakyThrows
    public Dumper load() {
        Files.createDirectories(Path.of(location));

        return new Dumper(
                new IntermediateSchemaMapper(unknownTypeSupport),
                new Playground(),
                new SqlGenerator(),
                Ignition.startClient(new ClientConfiguration().setAddresses(addrs)),
                location,
                snapshotName
        );
    }
}
