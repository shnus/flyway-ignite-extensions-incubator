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
/*
 * Copyright (C) Red Gate Software Ltd 2010-2022
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.ingvard.incubator.ignite.flyway.thin;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

/**
 * Apache Ignite implementation of Schema.
 */
public class IgniteThinSchema extends Schema<IgniteThinDatabase, IgniteThinTable> {
    /**
     * Logger.
     */
    private static final Log LOG = LogFactory.getLog(IgniteThinSchema.class);

    /**
     * Creates a new Ignite schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param db           The database-specific support.
     * @param name         The name of the schema.
     */
    IgniteThinSchema(JdbcTemplate jdbcTemplate, IgniteThinDatabase db, String name) {
        super(jdbcTemplate, db, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name) > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doEmpty() {
        return allTables().length == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + database.quote(name)); //Unsupported now by Ignite
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP SCHEMA " + database.quote(name));  //Unsupported now by Ignite
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doClean() throws SQLException {
        for (Table table : allTables()) {
            table.drop();
        }

        List<String> seqNames = listObjectNames("SEQUENCE", "IS_GENERATED = false");
        for (String statement : generateDropStatements("SEQUENCE", seqNames)) {
            jdbcTemplate.execute(statement);
        }

        List<String> constantNames = listObjectNames("CONSTANT", "");
        for (String statement : generateDropStatements("CONSTANT", constantNames)) {
            jdbcTemplate.execute(statement);
        }

        List<String> domainNames = listObjectNames("DOMAIN", "");
        if (!domainNames.isEmpty()) {
            if (name.equals(database.getMainConnection().getCurrentSchema().getName())) {
                for (String statement : generateDropStatementsForCurrentSchema("DOMAIN", domainNames)) {
                    jdbcTemplate.execute(statement);
                }
            } else {
                LOG.error("Unable to drop DOMAIN objects in schema " + database.quote(name));
            }
        }
    }

    /**
     * Generate the statements for dropping all the objects of this type in this schema.
     *
     * @param objType  The type of object to drop (Sequence, constant, ...)
     * @param objNames The names of the objects to drop.
     * @return The list of statements.
     */
    private List<String> generateDropStatements(String objType, List<String> objNames) {
        List<String> statements = new ArrayList<>();

        for (String objectName : objNames) {
            String dropStatement =
                    "DROP " + objType + database.quote(name, objectName);

            statements.add(dropStatement);
        }

        return statements;
    }

    /**
     * Generate the statements for dropping all the objects of this type in the current schema.
     *
     * @param objType  The type of object to drop (Sequence, constant, ...)
     * @param objNames The names of the objects to drop.
     * @return The list of statements.
     */
    private List<String> generateDropStatementsForCurrentSchema(String objType, List<String> objNames) {
        List<String> statements = new ArrayList<>();

        for (String objectName : objNames) {
            String dropStatement =
                    "DROP " + objType + database.quote(objectName);

            statements.add(dropStatement);
        }

        return statements;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteThinTable[] doAllTables() throws SQLException {
        List<String> tblNames = listObjectNames("TABLE", "TABLE_TYPE = 'TABLE' AND TABLE_NAME !='__T0'");

        IgniteThinTable[] tbls = new IgniteThinTable[tblNames.size()];

        for (int i = 0; i < tblNames.size(); i++) {
            tbls[i] = new IgniteThinTable(jdbcTemplate, database, this, tblNames.get(i));
        }

        return tbls;
    }

    /**
     * List the names of the objects of this type in this schema.
     *
     * @param objType   The type of objects to list (Sequence, constant, ...)
     * @param qrySuffix Suffix to append to the query to find the objects to list.
     * @return The names of the objects.
     * @throws SQLException when the object names could not be listed.
     */
    private List<String> listObjectNames(String objType, String qrySuffix) throws SQLException {
        String qry = "SELECT " + objType + "_NAME FROM INFORMATION_SCHEMA." + objType
                + "S WHERE " + objType + "_SCHEMA = ?";
        if (StringUtils.hasLength(qrySuffix)) {
            qry += " AND " + qrySuffix;
        }

        return jdbcTemplate.queryForStringList(qry, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table getTable(String tblName) {
        return new IgniteThinTable(jdbcTemplate, database, this, tblName);
    }
}
