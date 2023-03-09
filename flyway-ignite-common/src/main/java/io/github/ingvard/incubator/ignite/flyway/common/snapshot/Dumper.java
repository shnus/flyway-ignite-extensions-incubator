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

import static io.github.ingvard.incubator.ignite.flyway.common.util.FileUtils.fileWithNextIndex;
import static io.github.ingvard.incubator.ignite.flyway.common.util.StringUtils.NEW_LINE;

import io.github.ingvard.incubator.ignite.flyway.common.sandbox.Playground;
import io.github.ingvard.incubator.ignite.flyway.common.snapshot.exception.SnapshotOperationException;
import io.github.ingvard.incubator.ignite.flyway.common.sql.generator.SqlGenerator;
import io.github.ingvard.incubator.ignite.flyway.common.sql.mapper.IntermediateSchemaMapper;
import io.github.ingvard.incubator.ignite.flyway.common.sql.objects.Table;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.client.IgniteClient;

/**
 * The starting point for managing snapshots in Ignite.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class Dumper {
    /**
     * Sql format ext.
     */
    public static final String SQL_FORMAT_EXT = ".sql";

    /**
     * Schema mapper.
     */
    private final IntermediateSchemaMapper schemaMapper;

    /**
     * Playground.
     */
    private final Playground playground;

    /**
     * Sql generator.
     */
    private final SqlGenerator sqlGenerator;

    /**
     * Target base client.
     */
    private final IgniteClient targetBaseClient;

    /**
     * Dump work directory.
     */
    private final String dumpWorkDir;

    /**
     * File name.
     */
    private final String fileName;

    /**
     * Creates a fluent builder of a Dumper instance.
     */
    public static DumperConfiguration configure() {
        return new DumperConfiguration();
    }

    /**
     * Creates a full schema snapshot that is stored in {@link DumperConfiguration#getLocation()}
     * with name {@link DumperConfiguration#getSnapshotName()}.
     */
    public SnapshotOperation createFullSchemaSnapshot() {
        return createFullSchemaSnapshot(fileName);
    }

    /**
     * See {@link Dumper#createFullSchemaSnapshot} doc.
     *
     * @param snapshotName Snapshot name.
     */
    public SnapshotOperation createFullSchemaSnapshot(String snapshotName) {
        File snapshotFile = fileWithNextIndex(Paths.get(dumpWorkDir, snapshotName + SQL_FORMAT_EXT).toFile(), SQL_FORMAT_EXT);

        try (RandomAccessFile stream = new RandomAccessFile(snapshotFile, "rw"); FileChannel channel = stream.getChannel()) {
            channel.tryLock(); // Lock release on try-with-resources.

            log.info("Full snapshot schema dump is started [optDir={}]", dumpWorkDir);

            List<String> dumpedCaches = new ArrayList<>();
            List<String> skippedCaches = new ArrayList<>();

            for (String cacheName : targetBaseClient.cacheNames()) {
                try {
                    Table table = schemaMapper.map(targetBaseClient.cache(cacheName).getConfiguration());

                    String createTableSql = sqlGenerator.generateCreateTableSql(table);

                    stream.write(createTableSql.getBytes(StandardCharsets.UTF_8));
                    stream.write(NEW_LINE.getBytes(StandardCharsets.UTF_8));

                    dumpedCaches.add(cacheName);
                } catch (Exception e) {
                    skippedCaches.add(cacheName);
                }
            }

            log.info("Full snapshot was created [optDir={}, dumpedCaches={}, skippedCaches={}]",
                    dumpWorkDir,
                    dumpedCaches,
                    skippedCaches
            );

            return new SnapshotOperation(true, snapshotFile.getName());
        } catch (OverlappingFileLockException e) {
            log.warn("The lock file cannot be obtained, a parallel snapshot is possible running.");

            throw new SnapshotOperationException();
        } catch (Exception e) {
            log.error("Snapshot creation failed with an error: " + e.getMessage(), e);

            throw new SnapshotOperationException();
        }
    }
}
