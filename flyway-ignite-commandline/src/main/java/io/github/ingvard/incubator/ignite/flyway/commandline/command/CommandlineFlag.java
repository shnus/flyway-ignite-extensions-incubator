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

package io.github.ingvard.incubator.ignite.flyway.commandline.command;

import java.util.List;
import lombok.Getter;

/**
 * Flag enum extender. Enum inheritance is disabled in Java, for rid of boilerplate you are able to use {@link Getter};
 */
public interface CommandlineFlag {
    /**
     * Flag aliases, for example: -run, -start and etc.
     */
    List<String> getAliases();

    /**
     * Checks that enum type is equals to one of flag aliases.
     *
     * @param str String in format: -flagName.
     */
    default boolean match(String str) {
        return getAliases().contains(str.substring(1));
    }
}
