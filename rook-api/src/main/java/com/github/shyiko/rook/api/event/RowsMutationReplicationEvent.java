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

import java.util.Collection;

/**
 * @param <T> type of collection for storing rows
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public abstract class RowsMutationReplicationEvent<T extends Collection> implements ReplicationEvent {

    protected final String schema;
    protected final String table;
    protected final T rows;

    protected RowsMutationReplicationEvent(String schema, String table, T rows) {
        this.schema = schema;
        this.table = table;
        this.rows = rows;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public T getRows() {
        return rows;
    }
}
