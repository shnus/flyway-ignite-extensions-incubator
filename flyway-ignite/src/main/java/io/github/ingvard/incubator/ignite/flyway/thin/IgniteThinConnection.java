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
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;

/**
 * Apache Ignite Thin connection.
 */
public class IgniteThinConnection extends Connection<IgniteThinDatabase> {
    /**
     * Default constructor.
     *
     * @param db   Database.
     * @param conn Connection.
     */
    IgniteThinConnection(IgniteThinDatabase db, java.sql.Connection conn) {
        super(db, conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        getJdbcConnection().setSchema(schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema getSchema(String name) {
        return new IgniteThinSchema(jdbcTemplate, database, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return getJdbcConnection().getSchema();
    }
}
