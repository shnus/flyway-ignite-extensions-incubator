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

import java.util.Arrays;
import java.util.List;
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
     * {@inheritDoc}
     */
    @Override public boolean handlesCommand(String cmd) {
        return SNAPSHOT_COMMANDS.contains(cmd);
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean handlesParameter(String param) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override public OperationResult handle(String cmd, Configuration cfg, List<String> flags) throws FlywayException {
        throw new UnsupportedOperationException("TODO");
    }

    /**
     * {@inheritDoc}
     */
    @Override public List<Pair<String, String>> getUsage() {
        return List.of(Pair.of(String.join(", ", SNAPSHOT_COMMANDS), "Make a schema snapshot"));
    }
}
