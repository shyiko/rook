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

import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.event.InsertRowReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.it.h4ftiom.model.OneToManyEntity;
import com.github.shyiko.rook.it.h4ftiom.model.RootEntity;
import com.github.shyiko.rook.source.mysql.MySQLReplicationStream;
import com.github.shyiko.rook.target.hibernate4.fulltextindex.DefaultEntityIndexer;
import com.github.shyiko.rook.target.hibernate4.fulltextindex.EntityIndexer;
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
import java.util.concurrent.TimeUnit;

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
        Class.forName("com.mysql.jdbc.Driver");
        ResourceBundle bundle = ResourceBundle.getBundle("master");
        recreateDatabase(bundle.getString("hibernate.connection.url"),
            bundle.getString("hibernate.connection.username"), bundle.getString("hibernate.connection.password"));
        // todo: make sure changes got replicated
        String dsURL = bundle.getString("hibernate.connection.url");
        URI uri = new URI("schema" + dsURL.substring(dsURL.indexOf("://")));
        replicationStream = new MySQLReplicationStream(uri.getHost(), uri.getPort()).
            authenticateWith(bundle.getString("hibernate.connection.username"),
                bundle.getString("hibernate.connection.password"));
        replicationStream.connect();
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
        replicationStream.registerListener(countDownReplicationListener = new CountDownReplicationListener());
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
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        ExecutionContext slaveContext = ExecutionContextHolder.get("slave");
        if (enableFTIS) {
            final SessionFactory sessionFactory = slaveContext.getSessionFactory();
            replicationStream.registerListener(new FullTextIndexSynchronizer(
                slaveContext.getConfiguration(),
                sessionFactory,
                new EntityIndexer() {

                    private EntityIndexer entityIndexer = new DefaultEntityIndexer(sessionFactory);

                    @Override
                    public void index(Class entityClass, Serializable id) {
                        entityIndexer.index(entityClass, id);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Indexed " + entityClass.getSimpleName() + "#" + id);
                        }
                    }
                })
            );
        }
        masterContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.persist(new RootEntity(
                    "Slytherin",
                    new HashSet<OneToManyEntity>(Arrays.asList(
                        new OneToManyEntity("Draco Malfoy"),
                        new OneToManyEntity("Vincent Crabbe"),
                        new OneToManyEntity("Gregory Goyle")
                    ))
                ));
            }
        });
        countDownReplicationListener.waitFor(InsertRowReplicationEvent.class, 7, DEFAULT_TIMEOUT);
        slaveContext.execute(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                FullTextSession fullTextSession = Search.getFullTextSession(session);
                QueryBuilder qb = fullTextSession.getSearchFactory().
                    buildQueryBuilder().forEntity(RootEntity.class).get();
                Query luceneQuery = qb.keyword().onField("name").matching("Slytherin").createQuery();
                FullTextQuery ftsQuery = fullTextSession.createFullTextQuery(luceneQuery, RootEntity.class);
                RootEntity rootEntity = (RootEntity) ftsQuery.uniqueResult();
                assertTrue(enableFTIS == (rootEntity != null));
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
