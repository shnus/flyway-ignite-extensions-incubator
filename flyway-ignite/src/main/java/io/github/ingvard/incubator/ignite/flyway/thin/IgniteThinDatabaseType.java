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

import java.sql.Connection;
import java.sql.Types;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

/**
 * Ignite thin database type.
 */
public class IgniteThinDatabaseType extends BaseDatabaseType {
    /**
     * Database name.
     */
    public static final String DATABASE_NAME = "Apache Ignite";

    /**
     * Plugin name.
     */
    public String getName() {
        return DATABASE_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith(JdbcThinUtils.URL_PREFIX);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean handlesDatabaseProductNameAndVersion(
            String dbProductName,
            String dbProductVer,
            Connection conn
    ) {
        return dbProductName.startsWith(DATABASE_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDriverClass(String url, ClassLoader clsLdr) {
        return IgniteJdbcThinDriver.class.getCanonicalName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Database createDatabase(
            Configuration configuration,
            JdbcConnectionFactory jdbcConnFactory,
            StatementInterceptor statementInterceptor
    ) {
        return new IgniteThinDatabase(configuration, jdbcConnFactory, statementInterceptor);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Parser createParser(
            Configuration configuration,
            ResourceProvider rsrcProvider,
            ParsingContext parsingCtx
    ) {
        return new IgniteThinParser(configuration, parsingCtx);
    }
}
