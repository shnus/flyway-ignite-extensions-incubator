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

package io.github.ingvard.incubator.ignite.flyway.common.sql.generator;

import static io.github.ingvard.incubator.ignite.flyway.common.util.StringUtils.NEW_LINE;
import static io.github.ingvard.incubator.ignite.flyway.common.util.StringUtils.PADDING;
import static io.github.ingvard.incubator.ignite.flyway.common.util.StringUtils.WHITESPACE;
import static java.util.stream.Collectors.joining;

import io.github.ingvard.incubator.ignite.flyway.common.sql.objects.Column;
import io.github.ingvard.incubator.ignite.flyway.common.sql.objects.Table;
import io.github.ingvard.incubator.ignite.flyway.common.sql.objects.TableStoreConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Sql Generator.
 */
@Slf4j
@RequiredArgsConstructor
public class SqlGenerator {
    /**
     * Generate table creation sql.
     *
     * @return Sql statement.
     */
    public String generateCreateTableSql(Table table) {
        return String.format("CREATE TABLE IF NOT EXISTS %s (" + NEW_LINE
                        + "%s"
                        + NEW_LINE + ") WITH \"%s\";",
                table.getName(),
                columnsInlineBlock(table),
                tableStoreConfig(table.getConfig())
        );
    }

    /**
     * Inline block with sql definitions of columns.
     *
     * @param table Table.
     */
    private String columnsInlineBlock(Table table) {
        return table.getColumns().stream()
                .map(this::tableColumn)
                .map(c -> PADDING + c)
                .collect(Collectors.joining("," + NEW_LINE));
    }

    /**
     * Return sql definition of a table configuration.
     *
     * @param storeCfg Store config.
     */
    private Object tableStoreConfig(TableStoreConfig storeCfg) {
        Map<String, String> arguments = new HashMap<>();

        arguments.put("cache_name", storeCfg.getCacheName());

        if (storeCfg.getCacheGroup() != null) {
            arguments.put("cache_group", storeCfg.getCacheGroup());
        }

        if (storeCfg.getDataRegion() != null) {
            arguments.put("data_region", storeCfg.getDataRegion());
        }

        if (storeCfg.getParallelism() != CacheConfiguration.DFLT_QUERY_PARALLELISM) {
            arguments.put("parallelism", String.valueOf(storeCfg.getParallelism()));
        }

        if (storeCfg.getBackups() != CacheConfiguration.DFLT_BACKUPS) {
            arguments.put("backups", String.valueOf(storeCfg.getBackups()));
        }

        if (storeCfg.getAtomicity() != null) {
            arguments.put("atomicity", storeCfg.getAtomicity().toString());
        }

        if (storeCfg.getWriteSynchronizationMode() != CacheWriteSynchronizationMode.PRIMARY_SYNC) {
            arguments.put("write_synchronization_mode", storeCfg.getWriteSynchronizationMode().toString());
        }

        //TODO Add hint for table configuration compaction according to a template

        //TODO if (ccfg == null) {
        //TODO   if (QueryUtils.TEMPLATE_PARTITIONED.equalsIgnoreCase(templateName))
        //TODO       ccfg = new CacheConfiguration<>().setCacheMode(CacheMode.PARTITIONED);
        //TODO    else if (QueryUtils.TEMPLATE_REPLICATED.equalsIgnoreCase(templateName))
        //TODO       ccfg = new CacheConfiguration<>().setCacheMode(CacheMode.REPLICATED);
        //TODO   else
        //TODO      throw new SchemaOperationException(SchemaOperationException.CODE_CACHE_NOT_FOUND, templateName);
        //TODO ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);


        //TODO add arguments
        //TODO cacheCfgTemplate
        //TODO KEY_TYPE
        //TODO VALUE_TYPE
        //TODO WRAP_KEY
        //TODO WRAP_VALUE
        //TODO ENCRYPTED
        //TODO PK_INLINE_SIZE
        //TODO AFFINITY_INDEX_INLINE_SIZE

        return arguments.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(joining(","));
    }

    /**
     * Returns sql definition of column.
     *
     * @param column Column.
     */
    protected String tableColumn(Column column) {
        StringBuilder columnDescription = new StringBuilder()
                .append(column.getName())
                .append(WHITESPACE)
                .append(column.getSqlType());

        //TODO type scale

        if (column.isPrimary()) {
            columnDescription.append(WHITESPACE).append("PRIMARY KEY");
        }

        if (column.isNullable()) {
            columnDescription.append(WHITESPACE).append("NOT NULL");
        }

        return columnDescription.toString();
    }
}
