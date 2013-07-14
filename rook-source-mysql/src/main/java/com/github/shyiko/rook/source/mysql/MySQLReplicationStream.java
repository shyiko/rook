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

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.rook.api.ConnectionException;
import com.github.shyiko.rook.api.ReplicationListener;
import com.github.shyiko.rook.api.ReplicationStream;
import com.github.shyiko.rook.api.event.DeleteRowReplicationEvent;
import com.github.shyiko.rook.api.event.GroupOfReplicationEvents;
import com.github.shyiko.rook.api.event.InsertRowReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowReplicationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class MySQLReplicationStream implements ReplicationStream {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String hostname = "localhost";
    private int port = 3306;
    private String username;
    private String password;

    private BinaryLogClient binaryLogClient;

    private final List<ReplicationListener> listeners = new LinkedList<ReplicationListener>();
    private final Map<Long, TableMapEventData> tablesById = new HashMap<Long, TableMapEventData>();

    public MySQLReplicationStream() {
    }

    public MySQLReplicationStream(String hostname) {
        this();
        this.hostname = hostname;
    }

    public MySQLReplicationStream(String hostname, int port) {
        this();
        this.hostname = hostname;
        this.port = port;
    }

    public MySQLReplicationStream authenticateWith(String username, String password) {
        if (isConnected()) {
            throw new IllegalStateException(
                    "Replication stream needs to be disconnected before authentication details can be changed");
        }
        this.username = username;
        this.password = password;
        return this;
    }

    @Override
    public void connect() throws ConnectionException {
        if (binaryLogClient != null) {
            throw new IllegalStateException();
        }
        binaryLogClient = new BinaryLogClient(hostname, port, username, password);
        binaryLogClient.registerEventListener(new DelegatingEventListener());
        try {
            binaryLogClient.connect(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ConnectionException("Failed to establish connection to the replication stream", e);
        }
    }

    @Override
    public boolean isConnected() {
        return binaryLogClient != null && binaryLogClient.isConnected();
    }

    @Override
    public MySQLReplicationStream registerListener(ReplicationListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
        return this;
    }

    public void unregisterListener(Class<? extends ReplicationListener> listenerClass) {
        synchronized (listeners) {
            Iterator<ReplicationListener> iterator = listeners.iterator();
            while (iterator.hasNext()) {
                ReplicationListener replicationListener = iterator.next();
                if (listenerClass.isInstance(replicationListener)) {
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public void disconnect() throws ConnectionException {
        if (binaryLogClient != null) {
            try {
                binaryLogClient.disconnect();
                binaryLogClient = null;
            } catch (Exception e) {
                throw new ConnectionException("Failed to disconnect from the replication stream", e);
            }
        }
    }

    private void notifyListeners(List<ReplicationEvent> events) {
        int numberOfEvents = events.size();
        if (numberOfEvents != 0) {
            notifyListeners(numberOfEvents == 1 ? events.get(0) :
                    new GroupOfReplicationEvents(events));
        }
    }

    private void notifyListeners(ReplicationEvent event) {
        synchronized (listeners) {
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

    private final class DelegatingEventListener implements BinaryLogClient.EventListener {

        @Override
        public void onEvent(Event event) {
            // todo: do something about schema changes
            switch (event.getHeader().getEventType()) {
                case TABLE_MAP:
                    TableMapEventData tableMapEventData = event.getData();
                    tablesById.put(tableMapEventData.getTableId(), tableMapEventData);
                    break;
                case PRE_GA_WRITE_ROWS:
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                    handleWriteRowsEvent(event);
                    break;
                case PRE_GA_UPDATE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                    handleUpdateRowsEvent(event);
                    break;
                case PRE_GA_DELETE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    handleDeleteRowsEvent(event);
                    break;
                default:
                    // ignore
            }
        }

        private void handleWriteRowsEvent(Event event) {
            WriteRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            List<Serializable[]> rows = eventData.getRows();
            List<ReplicationEvent> replicationEvents = new ArrayList<ReplicationEvent>(rows.size());
            for (Serializable[] row : rows) {
                replicationEvents.add(new InsertRowReplicationEvent(tableMapEvent.getDatabase(),
                        tableMapEvent.getTable(), row));
            }
            notifyListeners(replicationEvents);
        }

        private void handleUpdateRowsEvent(Event event) {
            UpdateRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            List<Map.Entry<Serializable[], Serializable[]>> rows = eventData.getRows();
            List<ReplicationEvent> replicationEvents = new ArrayList<ReplicationEvent>(rows.size());
            for (Map.Entry<Serializable[], Serializable[]> row : rows) {
                replicationEvents.add(new UpdateRowReplicationEvent(tableMapEvent.getDatabase(),
                        tableMapEvent.getTable(), row.getKey(), row.getValue()));
            }
            notifyListeners(replicationEvents);
        }

        private void handleDeleteRowsEvent(Event event) {
            DeleteRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            List<Serializable[]> rows = eventData.getRows();
            List<ReplicationEvent> replicationEvents = new ArrayList<ReplicationEvent>(rows.size());
            for (Serializable[] row : rows) {
                replicationEvents.add(new DeleteRowReplicationEvent(tableMapEvent.getDatabase(),
                        tableMapEvent.getTable(), row));
            }
            notifyListeners(replicationEvents);
        }

    }
}
