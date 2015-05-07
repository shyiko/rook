/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.rook.target.hibernate4.mapping;

import org.hibernate.mapping.Table;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.jdbc.connections.spi.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:igor.grunskyi@gmail.com">Igor Grunskiy</a>
 */
public class ColumnOrderMappingExporter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private ServiceRegistry serviceRegistry;

    public ColumnOrderMappingExporter(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    public Map<String, Integer> extractColumnMapping(Table table) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        try {
            ResultSet rs = null;
            Connection connection = null;
            try {
                connection = serviceRegistry.getService(ConnectionProvider.class).getConnection();
                DatabaseMetaData meta = connection.getMetaData();
                rs = meta.getColumns(table.getCatalog(), table.getSchema(), table.getName(), "%");
                int index = 0;
                while (rs.next()) {
                    String column = rs.getString("COLUMN_NAME");

                    if (column != null) {
                        result.put(column, index++);
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            logger.warn(e.getMessage(), e);
        }

        return result;
    }

}
