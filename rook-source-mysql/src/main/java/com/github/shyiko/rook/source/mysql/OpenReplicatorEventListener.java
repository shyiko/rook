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
import com.github.shyiko.rook.api.event.GroupOfReplicationEvents;
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
import com.google.code.or.common.glossary.column.StringColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
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
            ReplicationListener replicationListener = iterator.next();
            if (listenerClass.isInstance(replicationListener)) {
                iterator.remove();
            }
        }
    }

    @Override
    public void onEvents(BinlogEventV4 event) {
        if (event instanceof TableMapEvent) {
            TableMapEvent nativeEvent = (TableMapEvent) event;
            tableMap.put(nativeEvent.getTableId(), nativeEvent);
        } else
        if (event instanceof WriteRowsEvent) {
            handleWriteRowsEvent((WriteRowsEvent) event);
        } else
        if (event instanceof UpdateRowsEvent) {
            handleUpdateRowsEvent((UpdateRowsEvent) event);
        } else
        if (event instanceof DeleteRowsEvent) {
            handleDeleteRowsEvent((DeleteRowsEvent) event);
        }
    }

    private void handleWriteRowsEvent(WriteRowsEvent event) {
        List<Row> rows = event.getRows();
        List<ReplicationEvent> replicationEvents = new ArrayList<ReplicationEvent>(rows.size());
        for (Row row : rows) {
            TableMapEvent tableMapEvent = tableMap.get(event.getTableId());
            replicationEvents.add(new InsertRowReplicationEvent(tableMapEvent.getDatabaseName().toString(),
                tableMapEvent.getTableName().toString(), columnsOf(row)));
        }
        notifyListeners(replicationEvents);
    }

    private void handleUpdateRowsEvent(UpdateRowsEvent event) {
        List<Pair<Row>> rows = event.getRows();
        List<ReplicationEvent> replicationEvents = new ArrayList<ReplicationEvent>(rows.size());
        for (Pair<Row> row : rows) {
            TableMapEvent tableMapEvent = tableMap.get(event.getTableId());
            replicationEvents.add(new UpdateRowReplicationEvent(tableMapEvent.getDatabaseName().toString(),
                    tableMapEvent.getTableName().toString(), columnsOf(row.getBefore()), columnsOf(row.getAfter())));
        }
        notifyListeners(replicationEvents);
    }

    private void handleDeleteRowsEvent(DeleteRowsEvent event) {
        List<Row> rows = event.getRows();
        List<ReplicationEvent> replicationEvents = new ArrayList<ReplicationEvent>(rows.size());
        for (Row row : rows) {
            TableMapEvent tableMapEvent = tableMap.get(event.getTableId());
            replicationEvents.add(new DeleteRowReplicationEvent(tableMapEvent.getDatabaseName().toString(),
                    tableMapEvent.getTableName().toString(), columnsOf(row)));
        }
        notifyListeners(replicationEvents);
    }

    private Serializable[] columnsOf(Row row) {
        List<Column> columns = row.getColumns();
        Serializable[] columnsSerialized = new Serializable[columns.size()];
        int index = 0;
        for (Column column : columns) {
            columnsSerialized[index++] = column instanceof StringColumn ? column.toString() :
                (Serializable) column.getValue();
        }
        return columnsSerialized;
    }

    private synchronized void notifyListeners(List<ReplicationEvent> events) {
        int numberOfEvents = events.size();
        if (numberOfEvents != 0) {
            notifyListeners(numberOfEvents == 1 ? events.get(0) :
                    new GroupOfReplicationEvents(events));
        }
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
