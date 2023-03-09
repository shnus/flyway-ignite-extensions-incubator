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

package io.github.ingvard.incubator.ignite.flyway.common.sql.mapper;

import io.github.ingvard.incubator.ignite.flyway.common.sql.SqlTypeMapping;
import io.github.ingvard.incubator.ignite.flyway.common.sql.objects.Column;
import io.github.ingvard.incubator.ignite.flyway.common.sql.objects.Table;
import io.github.ingvard.incubator.ignite.flyway.common.sql.objects.TableStoreConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;

/**
 * Schema mapper for {@link QueryEntity} definition.
 */
@Slf4j
@RequiredArgsConstructor
public class IntermediateSchemaMapper {
    /**
     * Unknown type support.
     */
    private final boolean unknownTypeSupport;

    /**
     * This code reverts H2 Parser an intermediate representation to a sql statement. The logic of parsing is located here:
     * {@link org.apache.ignite.internal.processors.query.h2.CommandProcessor#runCommandH2}.
     * The DDL of a table creation: <a href="https://ignite.apache.org/docs/latest/sql-reference/ddl">SQL DDL</a>
     *
     * @param cacheCfg Cache config.
     */
    public Table map(ClientCacheConfiguration cacheCfg) {
        QueryEntity[] qryEntities = cacheCfg.getQueryEntities();

        if (qryEntities.length != 1) {
            throw new IllegalStateException("Table cache must have only one query entry definition [entities="
                    + Arrays.toString(qryEntities) + "]");
        }

        QueryEntity qryEntity = qryEntities[0];

        return new Table(
                qryEntity.getTableName(),
                columns(qryEntity),
                tableStoreConfig(cacheCfg)
        );
    }

    /**
     * Extracts table config from a cache configuration.
     *
     * @param cacheCfg Cache config.
     */
    public TableStoreConfig tableStoreConfig(ClientCacheConfiguration cacheCfg) {
        return new TableStoreConfig(
                cacheCfg.getName(),
                cacheCfg.getGroupName(),
                cacheCfg.getDataRegionName(),
                cacheCfg.getQueryParallelism(),
                cacheCfg.getBackups(),
                cacheCfg.getAtomicityMode().toString(),
                cacheCfg.getWriteSynchronizationMode()
        );
    }

    /**
     * Extracts columns definitions from {@link QueryEntity}.
     *
     * @param qryEntity Query entity.
     */
    public List<Column> columns(QueryEntity qryEntity) {
        List<Column> columns = new ArrayList<>();

        for (Map.Entry<String /* name */, String /* java type */> column : qryEntity.getFields().entrySet()) {
            columns.add(column(column.getKey(), column.getValue(), qryEntity));
        }

        return columns;
    }

    /**
     * Returns a column representation.
     *
     * @param columnName        Column name.
     * @param columnTypeClsName Column type class name.
     * @param qryEntity         Query entity.
     */
    public Column column(String columnName, String columnTypeClsName, QueryEntity qryEntity) {
        return new Column(
                columnName,
                sqlType(columnName, columnTypeClsName),
                qryEntity.getFieldsPrecision().get(columnName),
                qryEntity.getKeyFieldName().equals(columnName),
                qryEntity.getNotNullFields().contains(columnName)
        );
    }

    /**
     * Returns sql type by java row type name.
     *
     * @param columnName        Column name.
     * @param columnTypeClsName Column type class name.
     */
    private String sqlType(String columnName, String columnTypeClsName) {
        String jdbcType = JdbcThinUtils.typeName(columnTypeClsName);

        if (!jdbcType.equals(SqlTypeMapping.sqlUnknownType())) {
            return jdbcType;
        }

        Optional<String> typeByClsNameOpt = SqlTypeMapping.findSqlTypeByClassName(columnTypeClsName);

        if (typeByClsNameOpt.isEmpty() && unknownTypeSupport) {
            log.warn("Unsupported sql type was replaced to upper Object type [column={}, type={}].", columnName, columnTypeClsName);

            return SqlTypeMapping.sqlUnknownType();
        } else {
            return typeByClsNameOpt.orElseThrow(() -> new IllegalStateException(String.format(
                    "Unsupported sql type [column=%s, type=%s].", columnName, columnTypeClsName
            )));
        }
    }
}
