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
package com.github.shyiko.rook.it.h4ftiom;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.event.DeleteRowsReplicationEvent;
import com.github.shyiko.rook.api.event.InsertRowsReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowsReplicationEvent;
import com.github.shyiko.rook.it.h4ftiom.model.ManyToManyEntity;
import com.github.shyiko.rook.it.h4ftiom.model.RootEntity;
import com.github.shyiko.rook.source.mysql.MySQLReplicationStream;
import com.github.shyiko.rook.target.hibernate4.fulltextindex.FullTextIndexSynchronizer;
import org.apache.lucene.search.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.query.dsl.QueryBuilder;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class IntegrationTest {

    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    private final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);
    private MySQLReplicationStream replicationStream;
    private CountDownReplicationListener countDownReplicationListener;

    @BeforeClass
    public void setUp() throws Exception {
        ResourceBundle bundle = ResourceBundle.getBundle("slave");
        String dsURL = bundle.getString("hibernate.connection.url");
        URI uri = new URI("schema" + dsURL.substring(dsURL.indexOf("://")));
        final CountDownLatch latchForCreateDatabase = new CountDownLatch(1);
        replicationStream = new MySQLReplicationStream(uri.getHost(), uri.getPort(),
            bundle.getString("hibernate.connection.username"), bundle.getString("hibernate.connection.password")) {

            @Override
            protected void configureBinaryLogClient(final BinaryLogClient binaryLogClient) {
                binaryLogClient.registerEventListener(new BinaryLogClient.EventListener() {

                    @Override
                    public void onEvent(Event event) {
                        if (event.getHeader().getEventType() == EventType.QUERY &&
                            ((QueryEventData) event.getData()).getSql().toLowerCase().contains("create database")) {
                            latchForCreateDatabase.countDown();
                            if (latchForCreateDatabase.getCount() == 0) {
                                binaryLogClient.unregisterEventListener(this);
                            }
                        }
                    }
                });
            }
        };
        replicationStream.connect(DEFAULT_TIMEOUT);
        Class.forName("com.mysql.jdbc.Driver");
        recreateDatabaseForProfiles("master");
        assertTrue(latchForCreateDatabase.await(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS));
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
    public void testFullTextIndexIsNotUpdatedWithoutFTIS() throws Exception {
        testFullTextIndexUpdate(false);
    }

    @Test
    public void testFullTextIndexIsUpdatedByFTIS() throws Exception {
        testFullTextIndexUpdate(true);
    }

    private void testFullTextIndexUpdate(final boolean enableFTIS) throws Exception {
        ExecutionContext masterContext = ExecutionContextHolder.get("master"),
            slaveContext = ExecutionContextHolder.get("slave");
        if (enableFTIS) {
            SessionFactory sessionFactory = slaveContext.getSessionFactory();
            replicationStream.registerListener(new FullTextIndexSynchronizer(slaveContext.getConfiguration(),
                sessionFactory));
        }
        replicationStream.registerListener(countDownReplicationListener = new CountDownReplicationListener());
        testFullTextIndexUpdateOnInsert(masterContext, slaveContext, enableFTIS);
        testFullTextIndexUpdateOnUpdate(masterContext, slaveContext, enableFTIS);
        testFullTextIndexUpdateOnDelete(masterContext, slaveContext, enableFTIS);
    }

    private void testFullTextIndexUpdateOnInsert(ExecutionContext masterContext, ExecutionContext slaveContext,
            final boolean enableFTIS) throws TimeoutException, InterruptedException {
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.persist(new RootEntity("Slytherin"));
                session.persist(new RootEntity("Gryffindor"));
            }
        });
        countDownReplicationListener.waitFor(InsertRowsReplicationEvent.class, 2, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertTrue(enableFTIS == (searchRootEntityUsingFTI(session, "name", "Slytherin") != null));
            }
        });
    }

    private void testFullTextIndexUpdateOnDelete(ExecutionContext masterContext, ExecutionContext slaveContext,
            final boolean enableFTIS) throws TimeoutException, InterruptedException {
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertTrue(enableFTIS == (searchRootEntityUsingFTI(session, "name", "Gryffindor") != null));
            }
        });
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.createQuery("delete from RootEntity where name = 'Gryffindor'").executeUpdate();
            }
        });
        countDownReplicationListener.waitFor(DeleteRowsReplicationEvent.class, 1, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertNull(searchRootEntityUsingFTI(session, "name", "Gryffindor"));
            }
        });
    }

    private void testFullTextIndexUpdateOnUpdate(ExecutionContext masterContext, ExecutionContext slaveContext,
            final boolean enableFTIS) throws TimeoutException, InterruptedException {
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.createQuery("update RootEntity set name = 'Hufflepuff' where name = 'Slytherin'").
                    executeUpdate();
            }
        });
        countDownReplicationListener.waitFor(UpdateRowsReplicationEvent.class, 1, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertNull(searchRootEntityUsingFTI(session, "name", "Slytherin"));
                assertTrue(enableFTIS == (searchRootEntityUsingFTI(session, "name", "Hufflepuff") != null));
            }
        });
    }

    private RootEntity searchRootEntityUsingFTI(Session session, String field, String value) {
        FullTextSession fullTextSession = Search.getFullTextSession(session);
        QueryBuilder qb = fullTextSession.getSearchFactory().
            buildQueryBuilder().forEntity(RootEntity.class).get();
        Query luceneQuery = qb.keyword().onField(field).matching(value).createQuery();
        FullTextQuery ftsQuery = fullTextSession.createFullTextQuery(luceneQuery, RootEntity.class);
        return  (RootEntity) ftsQuery.uniqueResult();
    }

    @Test
    public void testChangeToEmbeddedEntityDoesNotTriggerContainedReindexWithoutFTIS() throws Exception {
        testReindexTriggerByEmbeddedEntity(false);
    }

    @Test
    public void testChangeToEmbeddedEntityDoesTriggerContainedReindexIfFTISIsOn() throws Exception {
        testReindexTriggerByEmbeddedEntity(true);
    }

    private void testReindexTriggerByEmbeddedEntity(final boolean enableFTIS) throws Exception {
        ExecutionContext masterContext = ExecutionContextHolder.get("master"),
            slaveContext = ExecutionContextHolder.get("slave");
        if (enableFTIS) {
            SessionFactory sessionFactory = slaveContext.getSessionFactory();
            replicationStream.registerListener(new FullTextIndexSynchronizer(slaveContext.getConfiguration(),
                sessionFactory));
        }
        replicationStream.registerListener(countDownReplicationListener = new CountDownReplicationListener());
        final AtomicReference<Serializable> rootEntityId = new AtomicReference<Serializable>();
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                rootEntityId.set(session.save(new RootEntity(
                    "Slytherin",
                    new HashSet<ManyToManyEntity>(Arrays.asList(
                        new ManyToManyEntity("Draco Malfoy")
                    ))
                )));
            }
        });
        countDownReplicationListener.waitFor(InsertRowsReplicationEvent.class, 3, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertTrue(enableFTIS == (searchRootEntityUsingFTI(session, "manyToManyEntities.name",
                    "Draco Malfoy") != null));
            }
        });
        testReindexTriggerByEmbeddedEntityOnInsert(masterContext, slaveContext, rootEntityId.get(), enableFTIS);
        testReindexTriggerByEmbeddedEntityOnUpdate(masterContext, slaveContext, rootEntityId.get(), enableFTIS);
        testReindexTriggerByEmbeddedEntityOnDelete(masterContext, slaveContext, rootEntityId.get(), enableFTIS);
    }

    private void testReindexTriggerByEmbeddedEntityOnInsert(ExecutionContext masterContext,
            ExecutionContext slaveContext, final Serializable rootEntityId, final boolean enableFTIS)
            throws TimeoutException, InterruptedException {
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId);
                rootEntity.addManyToManyEntity(new ManyToManyEntity("Vincent Crabbe"));
                session.update(rootEntity);
            }
        });
        countDownReplicationListener.waitFor(InsertRowsReplicationEvent.class, 2, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertTrue(enableFTIS == (searchRootEntityUsingFTI(session, "manyToManyEntities.name",
                    "Vincent Crabbe") != null));
            }
        });
    }

    private void testReindexTriggerByEmbeddedEntityOnUpdate(ExecutionContext masterContext,
            ExecutionContext slaveContext, final Serializable rootEntityId, final boolean enableFTIS)
            throws TimeoutException, InterruptedException {
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId);
                ManyToManyEntity oneToManyEntity = rootEntity.getManyToManyEntityByName("Draco Malfoy");
                oneToManyEntity.setName("Gregory Goyle");
                session.update(rootEntity);
            }
        });
        countDownReplicationListener.waitFor(UpdateRowsReplicationEvent.class, 1, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertTrue(enableFTIS == (searchRootEntityUsingFTI(session, "manyToManyEntities.name",
                    "Gregory Goyle") != null));
            }
        });
    }

    private void testReindexTriggerByEmbeddedEntityOnDelete(ExecutionContext masterContext,
            ExecutionContext slaveContext, final Serializable rootEntityId, final boolean enableFTIS)
            throws TimeoutException, InterruptedException {
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId);
                ManyToManyEntity manyToManyEntity = rootEntity.getManyToManyEntityByName("Gregory Goyle");
                rootEntity.removeManyToManyEntity(manyToManyEntity);
                session.update(rootEntity);
            }
        });
        countDownReplicationListener.waitFor(DeleteRowsReplicationEvent.class, 1, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertNull(searchRootEntityUsingFTI(session, "manyToManyEntities.name", "Gregory Goyle"));
            }
        });
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
