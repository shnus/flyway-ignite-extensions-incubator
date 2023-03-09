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

package io.github.ingvard.incubator.ignite.flyway.commandline;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ingvard.incubator.ignite.flyway.commandline.command.snapshot.SnapshotCommandExtension;
import org.junit.jupiter.api.Test;

/**
 * Base integration test.
 */
public class CommandlineTest extends CommandlineAbstractTest {
    /**
     * Checks that ignite modules is loaded and printed in cli welcome output.
     */
    @Test
    public void shouldLoadIgnitePluginsToCli() {
        runCliWithoutArguments();

        assertThat(usageMessage(SnapshotCommandExtension.class))
                .hasSize(1)
                .first()
                .satisfies(p -> assertThatLog(LogModule.MAIN_CONSOLE).anyMatch(ll ->
                        ll.contains(p.getLeft()) && ll.contains(p.getRight())
                ));
    }
}
