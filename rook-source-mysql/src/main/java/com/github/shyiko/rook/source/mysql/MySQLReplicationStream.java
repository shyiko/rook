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

import com.github.shyiko.rook.api.ConnectionException;
import com.github.shyiko.rook.api.ReplicationListener;
import com.github.shyiko.rook.api.ReplicationStream;
import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.RotateEvent;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    private volatile String binLogFileName;
    private volatile long binLogPosition;
    private OpenReplicator replicator;

    public MySQLReplicationStream() {
        replicator = new OpenReplicator();
        replicator.setBinlogEventListener(new OpenReplicatorEventListener() {

            @Override
            public void onEvents(BinlogEventV4 event) {
                if (event instanceof RotateEvent) {
                    RotateEvent nativeEvent = (RotateEvent) event;
                    startFrom(nativeEvent.getBinlogFileName().toString(), nativeEvent.getBinlogPosition());
                } else {
                    binLogPosition = event.getHeader().getNextPosition();
                    super.onEvents(event);
                }
            }
        });
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
        this.username = username;
        this.password = password;
        return this;
    }

    public MySQLReplicationStream startFrom(String binLogFileName, long binLogPosition) {
        if (!StringUtils.equals(this.binLogFileName, binLogFileName) ||
            this.binLogPosition != binLogPosition) {
            this.binLogFileName = binLogFileName;
            this.binLogPosition = binLogPosition;
            if (logger.isTraceEnabled()) {
                logger.trace("Updated binlog position to " + (binLogFileName == null ? "<undefined>" : binLogFileName) +
                        "#" + binLogPosition);
            }
        }
        return this;
    }

    // todo(shyiko): getPosition()
    public MySQLReplicationStream resetPosition() {
        return startFrom(null, 0);
    }

    @Override
    public void connect() throws ConnectionException {
        replicator.setHost(hostname);
        replicator.setPort(port);
        replicator.setUser(username);
        replicator.setPassword(password);
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("MySQL driver seems to be missing. " +
                "Please make sure mysql-connector-java is on the classpath");
        }
        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port,
                username, password);
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet resultSet = statement.executeQuery("show variables like 'server_id';");
                    if (!resultSet.next()) {
                        throw new ConnectionException("'server_id' is missing");
                    }
                    replicator.setServerId(resultSet.getInt("Value"));
                } finally {
                    statement.close();
                }
                if (binLogFileName == null) {
                    statement = connection.createStatement();
                    try {
                        ResultSet resultSet = statement.executeQuery("show master status;");
                        if (!resultSet.next()) {
                            throw new ConnectionException("Binlog file/position information is missing");
                        }
                        startFrom(resultSet.getString("File"), resultSet.getLong("Position"));
                    } finally {
                        statement.close();
                    }
                }
            } finally {
                connection.close();
            }
        } catch (SQLException e) {
            throw new ConnectionException("Failed to retrieve MySQL node information", e);
        }
        replicator.setBinlogFileName(binLogFileName);
        replicator.setBinlogPosition(binLogPosition);
        try {
            replicator.start();
        } catch (Exception e) {
            throw new ConnectionException("Failed to establish connection to the replication stream", e);
        }
    }

    @Override
    public boolean connected() {
        return replicator.isRunning();
    }

    @Override
    public MySQLReplicationStream registerListener(ReplicationListener listener) {
        ((OpenReplicatorEventListener) replicator.getBinlogEventListener()).addListener(listener);
        return this;
    }

    public void unregisterListener(Class<? extends ReplicationListener> listenerClass) {
        ((OpenReplicatorEventListener) replicator.getBinlogEventListener()).removeListener(listenerClass);
    }

    @Override
    public void disconnect() throws ConnectionException {
        try {
            replicator.stop(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ConnectionException("Failed to disconnect from replication stream", e);
        }
    }
}
