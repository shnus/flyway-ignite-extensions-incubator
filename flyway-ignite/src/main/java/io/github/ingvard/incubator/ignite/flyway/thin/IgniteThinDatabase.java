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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

/**
 * Apache Ignite database.
 */
public class IgniteThinDatabase extends Database<IgniteThinConnection> {
    /**
     * Default constructor.
     *
     * @param configuration         Configuration.
     * @param jdbcConnectionFactory Jdbc connection factory.
     * @param statementInterceptor  Statement interceptor.
     */
    public IgniteThinDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory,
                              StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteThinConnection doGetConnection(Connection conn) {
        return new IgniteThinConnection(this, conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected MigrationVersion determineVersion() {
        try {
            int buildId = getMainConnection().getJdbcTemplate()
                    .queryForInt("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = 'info.BUILD_ID'");

            return MigrationVersion.fromVersion(super.determineVersion().getVersion() + "." + buildId);
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to determine Apache Ignite build ID", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void ensureSupported() {
        notifyDatabaseIsNotFormallySupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getRawCreateScript(Table tbl, boolean baseline) {
        StringBuilder tableBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tbl).append(" (");

        tableBuilder.append("\"installed_rank\" INT PRIMARY KEY").append(",");
        tableBuilder.append("\"version\" VARCHAR(50)").append(",");
        tableBuilder.append("\"description\" VARCHAR(200) NOT NULL").append(",");
        tableBuilder.append("\"type\" VARCHAR(20) NOT NULL").append(",");
        tableBuilder.append("\"script\" VARCHAR(1000) NOT NULL").append(",");
        tableBuilder.append("\"checksum\" INT").append(",");
        tableBuilder.append("\"installed_by\" VARCHAR(100) NOT NULL").append(",");
        tableBuilder.append("\"installed_on\" TIMESTAMP NOT NULL").append(",");
        tableBuilder.append("\"execution_time\" INT NOT NULL").append(",");
        tableBuilder.append(" \"success\" BOOLEAN NOT NULL");
        tableBuilder.append(") WITH \"TEMPLATE=REPLICATED, BACKUPS=1, ATOMICITY=ATOMIC\";");

        if (baseline) {
            tableBuilder.append(getBaselineStatement(tbl)).append(";");
        }

        String idx = "CREATE INDEX IF NOT EXISTS \"" + tbl.getSchema().getName() + "\".\"" + tbl.getName() + "_s_idx\" ON " + tbl
                + " (\"success\");";

        return tableBuilder + " " + idx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSelectStatement(Table tbl) {
        return "SELECT " + quote("installed_rank") + "," + quote("version") + "," + quote("description") + "," + quote("type") + ","
                + quote("script") + "," + quote("checksum") + "," + quote("installed_on") + "," + quote("installed_by") + ","
                + quote("execution_time") + "," + quote("success") + " FROM " + tbl
                // Ignore special table created marker
                + " WHERE " + quote("type") + " != 'TABLE'" + " AND " + quote("installed_rank") + " > ?" + " ORDER BY "
                + quote("installed_rank");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getInsertStatement(Table tbl) {
        return "INSERT INTO " + tbl + " (" + quote("installed_rank") + ", " + quote("version") + ", " + quote("description") + ", "
                + quote("type") + ", " + quote("script") + ", " + quote("checksum") + ", " + quote("installed_by") + ", "
                + quote("installed_on") + ", " + quote("execution_time") + ", " + quote("success") + ")"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?,?)";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String doGetCurrentUser() {
        String userName;
        try {
            Field connPropsField = getMainConnection().getJdbcConnection().getClass().getDeclaredField("connProps");
            connPropsField.setAccessible(true);
            Object connProps = connPropsField.get(getMainConnection().getJdbcConnection());
            userName = (String) connProps.getClass().getMethod("getUsername").invoke(connProps);
            if (userName == null || userName.equals("")) {
                return "ignite";
            }
        } catch (NoSuchFieldException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new FlywayException(e);
        }
        return userName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useSingleConnection() {
        return super.useSingleConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMultiStatementTransactions() {
        return super.supportsMultiStatementTransactions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBooleanTrue() {
        return "1";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBooleanFalse() {
        return "0";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean catalogIsSchema() {
        return false;
    }
}
