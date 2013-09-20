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
package com.github.shyiko.rook.api.event;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class UpdateRowsReplicationEvent extends RowsMutationReplicationEvent<List<Map.Entry<Serializable[],
        Serializable[]>>> {

    public UpdateRowsReplicationEvent(String database, String table, List<Map.Entry<Serializable[],
            Serializable[]>> rows) {
        super(database, table, rows);
    }

    @SuppressWarnings("unchecked")
    public UpdateRowsReplicationEvent(String database, String table, Serializable[] previousValues,
            Serializable[] values) {
        super(database, table, Arrays.<Map.Entry<Serializable[], Serializable[]>>asList(
            new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(previousValues, values)));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UpdateRowsReplicationEvent");
        sb.append("{schema='").append(schema).append('\'');
        sb.append(", table='").append(table).append('\'');
        sb.append(", rows=[");
        if (!rows.isEmpty()) {
            for (Map.Entry<Serializable[], Serializable[]> row : rows) {
                sb.append("{").
                   append(Arrays.toString(row.getKey())).
                   append("->").
                   append(Arrays.toString(row.getValue())).
                   append("}, ");
            }
            int length = sb.length();
            sb.replace(length - 2, length, "");
        }
        sb.append("]}");
        return sb.toString();
    }

}
