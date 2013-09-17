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
package com.github.shyiko.rook.it.h4com;

import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.event.DeleteRowsReplicationEvent;
import com.github.shyiko.rook.api.event.InsertRowsReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowsReplicationEvent;
import com.github.shyiko.rook.it.h4com.model.OneToManyEntity;
import com.github.shyiko.rook.it.h4com.model.OneToOneEntity;
import com.github.shyiko.rook.it.h4com.model.RootEntity;
import com.github.shyiko.rook.source.mysql.MySQLReplicationStream;
import com.github.shyiko.rook.target.hibernate4.cache.HibernateCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate4.cache.QueryCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate4.cache.SecondLevelCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate4.cache.SynchronizationContext;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class IntegrationTest {

    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    private final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);
    private MySQLReplicationStream replicationStream;

    @BeforeClass
    public void setUp() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        recreateDatabaseForProfiles("master", "master-sdb");
        // todo: make sure changes got replicated
        ResourceBundle bundle = ResourceBundle.getBundle("slave");
        String dsURL = bundle.getString("hibernate.connection.url");
        URI uri = new URI("schema" + dsURL.substring(dsURL.indexOf("://")));
        replicationStream = new MySQLReplicationStream(uri.getHost(), uri.getPort(),
            bundle.getString("hibernate.connection.username"), bundle.getString("hibernate.connection.password"));
        replicationStream.connect(DEFAULT_TIMEOUT);
    }

    private void recreateDatabaseForProfiles(String... profiles) throws Exception {
        for (String profile : profiles) {
            ResourceBundle bundle = ResourceBundle.getBundle(profile);
            recreateDatabase(bundle.getString("hibernate.connection.url"),
                bundle.getString("hibernate.connection.username"), bundle.getString("hibernate.connection.password"));
        }
    }

    private void recreateDatabase(String connectionURI, String username, String password) throws Exception {
        int delimiter = connectionURI.lastIndexOf("/");
        Connection connection = DriverManager.getConnection(connectionURI.substring(0, delimiter),
            username, password);
        try {
            Statement statement = connection.createStatement();
            try {
                String databaseName = connectionURI.substring(delimiter + 1);
                statement.execute("drop database if exists " + databaseName);
                statement.execute("create database " + databaseName);
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    @BeforeMethod
    public void beforeTest() {
        replicationStream.registerListener(new ReplicationEventListener() {

            @Override
            public void onEvent(ReplicationEvent event) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Received " + event);
                }
            }
        });
    }

    @Test
    public void testQueryCacheIsNotEvictedWithoutQCS() throws Exception {
        testQueryCacheEviction(false);
    }

    @Test
    public void testQueryCacheIsEvictedByQCS() throws Exception {
        testQueryCacheEviction(true);
    }

    private void testQueryCacheEviction(final boolean enableQCS) throws Exception {
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        ExecutionContext slaveContext = ExecutionContextHolder.get("slave");
        if (enableQCS) {
            replicationStream.registerListener(
                new QueryCacheSynchronizer(new SynchronizationContext(slaveContext.getConfiguration(),
                        slaveContext.getSessionFactory()))
            );
        }
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener();
        replicationStream.registerListener(countDownReplicationListener);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 0);
            }
        });
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.persist(new RootEntity("Slytherin"));
            }
        });
        countDownReplicationListener.waitFor(InsertRowsReplicationEvent.class, 1, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(),
                    enableQCS ? 1 : 0);
            }
        });
    }

    @Test
    public void testSecondLevelCacheIsNotEvictedWithoutSLCS() throws Exception {
        testSecondLevelCacheEviction(false);
    }

    @Test
    public void testSecondLevelCacheIsEvictedBySLCS() throws Exception {
        testSecondLevelCacheEviction(true);
    }

    private void testSecondLevelCacheEviction(final boolean enableSLCS) throws Exception {
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        ExecutionContext slaveContext = ExecutionContextHolder.get("slave");
        if (enableSLCS) {
            replicationStream.registerListener(
                new SecondLevelCacheSynchronizer(new SynchronizationContext(slaveContext.getConfiguration(),
                        slaveContext.getSessionFactory()))
            );
        }
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener();
        replicationStream.registerListener(countDownReplicationListener);
        final AtomicReference<Serializable> rootEntityId = new AtomicReference<Serializable>();
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                rootEntityId.set(session.save(new RootEntity(
                    "Slytherin",
                    new OneToOneEntity("Severus Snape"),
                    new HashSet<OneToManyEntity>(Arrays.asList(
                        new OneToManyEntity("Draco Malfoy"),
                        new OneToManyEntity("Vincent Crabbe"),
                        new OneToManyEntity("Gregory Goyle")
                    ))
                )));
            }
        });
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId.get());
                assertEquals(rootEntity.getName(), "Slytherin");
            }
        });
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId.get());
                rootEntity.setName("Slytherin House");
                session.merge(rootEntity);
            }
        });
        countDownReplicationListener.waitFor(UpdateRowsReplicationEvent.class, 1, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId.get());
                assertEquals(rootEntity.getName(), enableSLCS ? "Slytherin House" : "Slytherin");
            }
        });
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.delete(session.get(RootEntity.class, rootEntityId.get()));
            }
        });
        countDownReplicationListener.waitFor(DeleteRowsReplicationEvent.class, 2, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId.get());
                assertTrue(enableSLCS == (rootEntity == null));
            }
        });
    }

    @Test
    public void testOnlyWiredInDatabaseIsAffected() throws Exception {
        class ReplicationContext {
            private ExecutionContext master, slave;
            ReplicationContext(ExecutionContext master, ExecutionContext slave) {
                this.master = master; this.slave = slave;
            }
        }
        final ReplicationContext primaryContext = new ReplicationContext(
                ExecutionContextHolder.get("master"), ExecutionContextHolder.get("slave")),
            separateContext = new ReplicationContext(
                ExecutionContextHolder.get("master-sdb"), ExecutionContextHolder.get("slave-sdb")
            );
        replicationStream.registerListener(new HibernateCacheSynchronizer(
                primaryContext.slave.getConfiguration(),
                primaryContext.slave.getSessionFactory()
        ));
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener();
        replicationStream.registerListener(countDownReplicationListener);
        for (ReplicationContext context : new ReplicationContext[] {primaryContext, separateContext}) {
            context.slave.execute(new Callback<Session>() {

                @Override
                public void execute(Session session) {
                    assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 0);
                }
            });
        }
        for (final ReplicationContext group : new ReplicationContext[] {primaryContext, separateContext}) {
            group.master.execute(new Callback<Session>() {

                @Override
                public void execute(Session session) {
                    session.save(new RootEntity("Slytherin"));
                }
            });
        }
        countDownReplicationListener.waitFor(InsertRowsReplicationEvent.class, 2, DEFAULT_TIMEOUT);
        primaryContext.slave.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 1);
            }
        });
        separateContext.slave.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 0);
            }
        });
    }

    @Test
    public void testReplicationEventsComeGroupedByStatement() throws Exception {
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener();
        replicationStream.registerListener(countDownReplicationListener);
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.persist(new RootEntity("Slytherin"));
                session.persist(new RootEntity("Hufflepuff"));
                session.persist(new RootEntity("Ravenclaw"));
            }
        });
        countDownReplicationListener.waitFor(InsertRowsReplicationEvent.class, 3, DEFAULT_TIMEOUT);
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.createQuery("update RootEntity set name = '~'").executeUpdate();
            }
        });
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.createQuery("delete RootEntity where name = 'Ravenclaw'").executeUpdate();
            }
        });
        countDownReplicationListener.waitFor(UpdateRowsReplicationEvent.class, 1, DEFAULT_TIMEOUT);
    }

    @AfterMethod(alwaysRun = true)
    public void afterTest() throws Exception {
        replicationStream.unregisterListener(ReplicationEventListener.class);
        for (ExecutionContext executionContext : ExecutionContextHolder.flush()) {
            executionContext.close();
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        replicationStream.disconnect();
    }

    private static class ExecutionContextHolder {

        private static Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>();

        public static ExecutionContext get(String profile) {
            ExecutionContext executionContext = map.get(profile);
            if (executionContext == null) {
                map.put(profile, executionContext = new ExecutionContext(profile));
            }
            return executionContext;
        }

        public static Collection<ExecutionContext> flush() {
            Collection<ExecutionContext> result = new ArrayList<ExecutionContext>(map.values());
            map.clear();
            return result;
        }
    }

    private static final class ExecutionContext {

        private final Logger logger = LoggerFactory.getLogger(ExecutionContext.class);

        private Configuration configuration;
        private SessionFactory sessionFactory;
        private String profile;

        private ExecutionContext(String profile) {
            configuration = new Configuration().configure("hibernate.cfg.xml");
            try {
                configuration.addProperties(getProperties(profile));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load properties for " + profile + " profile");
            }
            ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().
                    applySettings(configuration.getProperties()).buildServiceRegistry();
            sessionFactory = configuration.buildSessionFactory(serviceRegistry);
            this.profile = profile;
        }

        private Properties getProperties(String profile) throws IOException {
            InputStream inputStream = getClass().getResource("/" + profile + ".properties").openStream();
            try {
                Properties properties = new Properties();
                properties.load(inputStream);
                return properties;
            } finally {
                inputStream.close();
            }
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public SessionFactory getSessionFactory() {
            return sessionFactory;
        }

        public void execute(Callback<Session> callback) {
            if (logger.isDebugEnabled()) {
                logger.debug("Executing callback on " + profile);
            }
            Session session = sessionFactory.openSession();
            try {
                session.beginTransaction();
                callback.execute(session);
                session.getTransaction().commit();
            } finally {
                session.close();
            }
        }

        public void close() {
            sessionFactory.close();
        }
    }

    private interface Callback<T> {

        void execute(T obj);
    }

}
