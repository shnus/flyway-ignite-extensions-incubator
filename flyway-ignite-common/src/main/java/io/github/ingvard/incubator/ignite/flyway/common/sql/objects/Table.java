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

package io.github.ingvard.incubator.ignite.flyway.common.sql.objects;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The Java object that represents a table in an intermediate form, which simplifies the comparison operation.
 */
@Getter
@RequiredArgsConstructor
public class Table {
    /** Name. */
    private final String name;

    /** Columns. */
    private final List<Column> columns;

    /** Config. */
    @SuppressWarnings("JavaAbbreviationUsage")
    private final TableStoreConfig config;
}
