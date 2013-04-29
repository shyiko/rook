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
package com.github.shyiko.rook.source.mysql;

import com.github.shyiko.rook.api.ReplicationListener;
import com.github.shyiko.rook.api.event.DeleteRowReplicationEvent;
import com.github.shyiko.rook.api.event.InsertRowReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowReplicationEvent;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * todo: non-blocking synchronization
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class OpenReplicatorEventListener implements BinlogEventListener {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private List<ReplicationListener> listeners = new LinkedList<ReplicationListener>();
    private Map<Long, TableMapEvent> tableMap = new HashMap<Long, TableMapEvent>();

    public synchronized void addListener(ReplicationListener listener) {
        listeners.add(listener);
    }

    public synchronized void removeListener(Class<? extends ReplicationListener> listenerClass) {
        Iterator<ReplicationListener> iterator = listeners.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            if (listenerClass.isInstance(iterator)) {
                iterator.remove();
            }
        }
    }

    @Override
    public void onEvents(BinlogEventV4 event) {
        // todo: intercept create or alter table queries for table metadata reload
        if (event instanceof TableMapEvent) {
            TableMapEvent nativeEvent = (TableMapEvent) event;
            tableMap.put(nativeEvent.getTableId(), nativeEvent);
        } else
        if (event instanceof WriteRowsEvent) {
            WriteRowsEvent nativeEvent = (WriteRowsEvent) event;
            for (Row row : nativeEvent.getRows()) {
                TableMapEvent tableMapEvent = tableMap.get(nativeEvent.getTableId());
                notifyListeners(new InsertRowReplicationEvent(tableMapEvent.getDatabaseName().toString(),
                    tableMapEvent.getTableName().toString(), columnsOf(row)));
            }
        } else
        if (event instanceof UpdateRowsEvent) {
            UpdateRowsEvent nativeEvent = (UpdateRowsEvent) event;
            for (Pair<Row> row : nativeEvent.getRows()) {
                TableMapEvent tableMapEvent = tableMap.get(nativeEvent.getTableId());
                notifyListeners(new UpdateRowReplicationEvent(tableMapEvent.getDatabaseName().toString(),
                    tableMapEvent.getTableName().toString(), columnsOf(row.getBefore()), columnsOf(row.getAfter())));
            }
        } else
        if (event instanceof DeleteRowsEvent) {
            DeleteRowsEvent nativeEvent = (DeleteRowsEvent) event;
            for (Row row : nativeEvent.getRows()) {
                TableMapEvent tableMapEvent = tableMap.get(nativeEvent.getTableId());
                notifyListeners(new DeleteRowReplicationEvent(tableMapEvent.getDatabaseName().toString(),
                    tableMapEvent.getTableName().toString(), columnsOf(row)));
            }
        }
    }

    private String[] columnsOf(Row row) {
        List<Column> columns = row.getColumns();
        String[] columnsSerialized = new String[columns.size()];
        int index = 0;
        for (Column column : columns) {
            columnsSerialized[index++] = column.toString();
        }
        return columnsSerialized;
    }

    private synchronized void notifyListeners(ReplicationEvent event) {
        for (ReplicationListener listener : listeners) {
            try {
                listener.onEvent(event);
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn(listener + " choked on " + event, e);
                }
            }
        }
    }
}
