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

package io.github.ingvard.incubator.ignite.flyway.common.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

/**
 * Datatype mapping accoring to {@link org.h2.value.DataType}.
 */
@RequiredArgsConstructor
public enum SqlTypeMapping {
    /**
     * UUID.
     */
    UUID(java.util.UUID.class.getCanonicalName(), "UUID"),
    /**
     * String.
     */
    STRING(String.class.getCanonicalName(), "VARCHAR"),
    /**
     * Object.
     */
    OBJECT(Object.class.getCanonicalName(), "OBJECT");

    /**
     * Types.
     */
    private static final Map<String, String> TYPES = new HashMap<>();

    /**
     * Class name.
     */
    private final String clsName;

    /**
     * Sql name.
     */
    private final String sqlName;

    static {
        for (SqlTypeMapping value : values()) {
            TYPES.put(value.clsName, value.sqlName);
        }
    }

    /**
     * Returns sql column type.
     *
     * @param clsName Class name.
     */
    public static Optional<String> findSqlTypeByClassName(String clsName) {
        return Optional.ofNullable(TYPES.get(clsName));
    }

    /**
     * Default type for unknown fields.
     */
    public static String sqlObjectType() {
        return OBJECT.sqlName;
    }
}
