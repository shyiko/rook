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
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.ReplicationStream;
import com.github.shyiko.rook.api.event.DeleteRowsReplicationEvent;
import com.github.shyiko.rook.api.event.InsertRowsReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.TXReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowsReplicationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class MySQLReplicationStream implements ReplicationStream {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String hostname;
    private final int port;
    private final String username;
    private final String password;

    private BinaryLogClient binaryLogClient;

    private final List<ReplicationEventListener> listeners = new LinkedList<ReplicationEventListener>();

    private volatile boolean groupEventsByTX = true;

    public MySQLReplicationStream(String username, String password) {
        this("localhost", 3306, username, password);
    }

    public MySQLReplicationStream(String hostname, int port, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public void setGroupEventsByTX(boolean groupEventsByTX) {
        this.groupEventsByTX = groupEventsByTX;
    }

    @Override
    public void connect() throws IOException {
        allocateBinaryLogClient().connect();
    }

    @Override
    public void connect(long timeoutInMilliseconds) throws IOException, TimeoutException {
        allocateBinaryLogClient().connect(timeoutInMilliseconds);
    }

    private synchronized BinaryLogClient allocateBinaryLogClient() {
        if (isConnected()) {
            throw new IllegalStateException("MySQL replication stream is already open");
        }
        binaryLogClient = new BinaryLogClient(hostname, port, username, password);
        binaryLogClient.registerEventListener(new DelegatingEventListener());
        configureBinaryLogClient(binaryLogClient);
        return binaryLogClient;
    }

    protected void configureBinaryLogClient(BinaryLogClient binaryLogClient) {
        // template method
    }

    @Override
    public synchronized boolean isConnected() {
        return binaryLogClient != null && binaryLogClient.isConnected();
    }

    @Override
    public void registerListener(ReplicationEventListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    @Override
    public void unregisterListener(ReplicationEventListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    public void unregisterListener(Class<? extends ReplicationEventListener> listenerClass) {
        synchronized (listeners) {
            Iterator<ReplicationEventListener> iterator = listeners.iterator();
            while (iterator.hasNext()) {
                ReplicationEventListener replicationListener = iterator.next();
                if (listenerClass.isInstance(replicationListener)) {
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public synchronized void disconnect() throws IOException {
        if (binaryLogClient != null) {
            binaryLogClient.disconnect();
            binaryLogClient = null;
        }
    }

    private void notifyListeners(ReplicationEvent event) {
        synchronized (listeners) {
            for (ReplicationEventListener listener : listeners) {
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

        private final Map<Long, TableMapEventData> tablesById = new HashMap<Long, TableMapEventData>();
        private final List<ReplicationEvent> txQueue = new LinkedList<ReplicationEvent>();
        private boolean transactionInProgress;

        @Override
        public void onEvent(Event event) {
            // todo: do something about schema changes
            EventType eventType = event.getHeader().getEventType();
            switch (eventType) {
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
                case QUERY:
                    if (groupEventsByTX) {
                        QueryEventData queryEventData = event.getData();
                        String query = queryEventData.getSql();
                        if ("BEGIN".equals(query)) {
                            transactionInProgress = true;
                        }
                    }
                    break;
                case XID:
                    if (groupEventsByTX) {
                        notifyListeners(new TXReplicationEvent(new ArrayList<ReplicationEvent>(txQueue)));
                        txQueue.clear();
                        transactionInProgress = false;
                    }
                    break;
                default:
                    // ignore
            }
        }

        private void handleWriteRowsEvent(Event event) {
            WriteRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            enqueue(new InsertRowsReplicationEvent(tableMapEvent.getDatabase(),
                tableMapEvent.getTable(), eventData.getRows()));
        }

        private void handleUpdateRowsEvent(Event event) {
            UpdateRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            enqueue(new UpdateRowsReplicationEvent(tableMapEvent.getDatabase(),
                tableMapEvent.getTable(), eventData.getRows()));
        }

        private void handleDeleteRowsEvent(Event event) {
            DeleteRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            enqueue(new DeleteRowsReplicationEvent(tableMapEvent.getDatabase(),
                tableMapEvent.getTable(), eventData.getRows()));
        }

        private void enqueue(ReplicationEvent event) {
            if (groupEventsByTX && transactionInProgress) {
                txQueue.add(event);
            } else {
                notifyListeners(event);
            }
        }

    }
}
