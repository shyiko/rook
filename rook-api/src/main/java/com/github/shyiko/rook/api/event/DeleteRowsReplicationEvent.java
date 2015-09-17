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
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class DeleteRowsReplicationEvent extends RowsMutationReplicationEvent<List<Serializable[]>> {

    public DeleteRowsReplicationEvent(long serverId, String schema, String table, List<Serializable[]> rows) {
        super(serverId, schema, table, rows);
    }

    public DeleteRowsReplicationEvent(long serverId, String schema, String table, Serializable[] row) {
        super(serverId, schema, table, Arrays.asList(new Serializable[][]{row}));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DeleteRowsReplicationEvent");
        sb.append("{serverId=").append(serverId);
        sb.append(", schema='").append(schema).append('\'');
        sb.append(", table='").append(table).append('\'');
        sb.append(", rows=[");
        if (!rows.isEmpty()) {
            for (Serializable[] row : rows) {
                sb.append(Arrays.toString(row)).append(", ");
            }
            int length = sb.length();
            sb.replace(length - 2, length, "");
        }
        sb.append("]}");
        return sb.toString();
    }

}
