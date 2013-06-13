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
package com.github.shyiko.rook.it.hcom;

import com.github.shyiko.rook.api.ReplicationListener;
import com.github.shyiko.rook.api.event.GroupOfReplicationEvents;
import com.github.shyiko.rook.api.event.InsertRowReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowReplicationEvent;
import com.github.shyiko.rook.it.hcom.model.OneToManyEntity;
import com.github.shyiko.rook.it.hcom.model.OneToOneEntity;
import com.github.shyiko.rook.it.hcom.model.RootEntity;
import com.github.shyiko.rook.source.mysql.MySQLReplicationStream;
import com.github.shyiko.rook.source.mysql.ReplicationStreamPosition;
import com.github.shyiko.rook.target.hibernate.cache.HibernateCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate.cache.QueryCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate.cache.SecondLevelCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate.cache.SynchronizationContext;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class IntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);
    private MySQLReplicationStream replicationStream;

    @BeforeClass
    public void setUp() throws Exception {
        ResourceBundle bundle = ResourceBundle.getBundle("slave");
        String dsURL = bundle.getString("datasource.url");
        URI uri = new URI("schema" + dsURL.substring(dsURL.indexOf("://")));
        replicationStream = new MySQLReplicationStream(uri.getHost(), uri.getPort()).
            authenticateWith(bundle.getString("datasource.username"), bundle.getString("datasource.password"));
        replicationStream.connect();
    }

    @BeforeMethod
    public void beforeTest() {
        replicationStream.registerListener(new ReplicationListener() {

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
            LocalSessionFactoryBean sessionFactoryBean = masterContext.getBean(LocalSessionFactoryBean.class);
            Configuration configuration = sessionFactoryBean.getConfiguration();
            replicationStream.registerListener(
                new QueryCacheSynchronizer(new SynchronizationContext(configuration,
                        slaveContext.getBean(SessionFactory.class)))
            );
        }
        slaveContext.execute(slaveContext.new Callback() {

            @Override
            public void callback(Session session) {
                assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 0);
            }
        });
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener(
            InsertRowReplicationEvent.class, 1
        );
        replicationStream.registerListener(countDownReplicationListener);
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.persist(new RootEntity("Slytherin"));
            }
        });
        assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
        slaveContext.execute(slaveContext.new Callback() {

            @Override
            public void callback(Session session) {
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
            LocalSessionFactoryBean sessionFactoryBean = masterContext.getBean(LocalSessionFactoryBean.class);
            Configuration configuration = sessionFactoryBean.getConfiguration();
            replicationStream.registerListener(
                new SecondLevelCacheSynchronizer(new SynchronizationContext(configuration,
                        slaveContext.getBean(SessionFactory.class)))
            );
        }
        final AtomicReference<Serializable> rootEntityId = new AtomicReference<Serializable>();
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
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
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener(
            UpdateRowReplicationEvent.class, 1
        );
        replicationStream.registerListener(countDownReplicationListener);
        slaveContext.execute(slaveContext.new Callback() {

            @Override
            public void callback(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId.get());
                assertEquals(rootEntity.getName(), "Slytherin");
            }
        });
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId.get());
                rootEntity.setName("Slytherin House");
                session.merge(rootEntity);
            }
        });
        assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
        slaveContext.execute(slaveContext.new Callback() {

            @Override
            public void callback(Session session) {
                RootEntity rootEntity = (RootEntity) session.get(RootEntity.class, rootEntityId.get());
                assertEquals(rootEntity.getName(), enableSLCS ? "Slytherin House" : "Slytherin");
            }
        });
    }

    @Test
    public void testOnlyWiredInDatabaseIsAffected() throws Exception {
        class ReplicationContext {
            ExecutionContext master, slave;
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
                primaryContext.master.getBean(LocalSessionFactoryBean.class).getConfiguration(),
                primaryContext.slave.getBean(SessionFactory.class)
        ));
        for (ReplicationContext context : new ReplicationContext[] {primaryContext, separateContext}) {
            context.slave.execute(context.slave.new Callback() {

                @Override
                public void callback(Session session) {
                    assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 0);
                }
            });
        }
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 2
        );
        replicationStream.registerListener(countDownReplicationListener);
        for (final ReplicationContext group : new ReplicationContext[] {primaryContext, separateContext}) {
            group.master.execute(group.master.new Callback() {

                @Override
                public void callback(Session session) {
                    session.save(new RootEntity("Slytherin"));
                }
            });
        }
        assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
        primaryContext.slave.execute(primaryContext.slave.new Callback() {

            @Override
            public void callback(Session session) {
                assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 1);
            }
        });
        separateContext.slave.execute(separateContext.slave.new Callback() {

            @Override
            public void callback(Session session) {
                assertEquals(session.createQuery("from RootEntity").setCacheable(true).list().size(), 0);
            }
        });
    }

    @Test
    public void testReplicationEventsComeGroupedByStatement() throws Exception {
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        CountDownReplicationListener regCountDownReplicationListener = new CountDownReplicationListener(
                GroupOfReplicationEvents.class, 1
        ), countDownReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 3
        );
        replicationStream.registerListener(regCountDownReplicationListener);
        replicationStream.registerListener(countDownReplicationListener);
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.persist(new RootEntity("Slytherin"));
                session.persist(new RootEntity("Hufflepuff"));
                session.persist(new RootEntity("Ravenclaw"));
            }
        });
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.createQuery("update RootEntity set name = '~'").executeUpdate();
            }
        });
        assertTrue(regCountDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
        assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
    }

    @Test
    public void testFlushLogsIsAutomaticallyTakenCareOf() throws Exception {
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        ExecutionContext slaveContext = ExecutionContextHolder.get("slave");
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 2
        );
        replicationStream.registerListener(countDownReplicationListener);
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.persist(new RootEntity("Slytherin"));
            }
        });
        slaveContext.execute(slaveContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.createSQLQuery("flush logs").executeUpdate();
            }
        });
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.persist(new RootEntity("Hufflepuff"));
            }
        });
        assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
    }

    @Test
    public void testStreamCanBeSuspendedAndResumed() throws Exception {
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        ExecutionContext slaveContext = ExecutionContextHolder.get("slave");
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 1
        );
        replicationStream.registerListener(countDownReplicationListener);
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.persist(new RootEntity("Slytherin"));
            }
        });
        assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
        replicationStream.disconnect();
        slaveContext.execute(slaveContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.createSQLQuery("flush logs").executeUpdate();
            }
        });
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.persist(new RootEntity("Hufflepuff"));
            }
        });
        // todo(shyiko): we can miss Hufflepuff here because of replication latency
        CountDownReplicationListener invalidStateReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 2
        );
        replicationStream.registerListener(invalidStateReplicationListener);
        CountDownReplicationListener validStateReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 1
        );
        replicationStream.registerListener(validStateReplicationListener);
        replicationStream.connect();
        assertFalse(invalidStateReplicationListener.waitForCompletion(3, TimeUnit.SECONDS),
                "Received more events than expected");
        assertTrue(validStateReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
    }

    @Test
    public void testStreamCanBeRewind() throws Exception {
        ReplicationStreamPosition initialPosition = replicationStream.getPosition();
        ExecutionContext masterContext = ExecutionContextHolder.get("master");
        CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 1
        );
        replicationStream.registerListener(countDownReplicationListener);
        masterContext.execute(masterContext.new Callback() {

            @Override
            public void callback(Session session) {
                session.persist(new RootEntity("Slytherin"));
            }
        });
        assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
        replicationStream.disconnect();
        replicationStream.setPosition(initialPosition);
        replicationStream.connect();
        CountDownReplicationListener restoredReplicationListener = new CountDownReplicationListener(
                InsertRowReplicationEvent.class, 1
        );
        replicationStream.registerListener(restoredReplicationListener);
        assertTrue(restoredReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
    }

    @Test
    public void testAutomaticFailover() throws Exception {
        int reverseProxyPort = 33262;
        ResourceBundle bundle = ResourceBundle.getBundle("slave");
        String dsURL = bundle.getString("datasource.url");
        URI uri = new URI("schema" + dsURL.substring(dsURL.indexOf("://")));
        final TCPReverseProxy tcpReverseProxy = new TCPReverseProxy(reverseProxyPort, uri.getPort());
        try {
            bindInSeparateThread(tcpReverseProxy);
            ExecutionContext masterContext = ExecutionContextHolder.get("master");
            MySQLReplicationStream replicationStream = new MySQLReplicationStream("localhost", reverseProxyPort).
                    authenticateWith(bundle.getString("datasource.username"), bundle.getString("datasource.password"));
            try {
                CountDownReplicationListener countDownReplicationListener = new CountDownReplicationListener(
                        InsertRowReplicationEvent.class, 1
                );
                replicationStream.registerListener(countDownReplicationListener);
                replicationStream.connect();
                Thread.sleep(500); // giving replication stream a chance to start reading data from a socket
                tcpReverseProxy.unbind();
                masterContext.execute(masterContext.new Callback() {

                    @Override
                    public void callback(Session session) {
                        session.persist(new RootEntity("Slytherin"));
                    }
                });
                bindInSeparateThread(tcpReverseProxy);
                assertTrue(countDownReplicationListener.waitForCompletion(3, TimeUnit.SECONDS));
            } finally {
                replicationStream.disconnect();
            }
        } finally {
            tcpReverseProxy.unbind();
        }
    }

    private void bindInSeparateThread(final TCPReverseProxy tcpReverseProxy) throws InterruptedException {
        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    tcpReverseProxy.bind();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        tcpReverseProxy.await(3, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void afterTest() throws Exception {
        replicationStream.unregisterListener(ReplicationListener.class);
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

        private GenericXmlApplicationContext context;
        private String profile;

        private ExecutionContext(String profile) {
            context = new GenericXmlApplicationContext();
            context.getEnvironment().setActiveProfiles(this.profile = profile);
            context.load("classpath:spring-context.xml");
            int numberOfRetries = 3,
                timeoutBetweenRetriesInMs = 1000;
            while (numberOfRetries-- > 0) {
                try {
                    context.refresh();
                    break;
                } catch (Exception e) {
                    logger.warn("Failed to start " + profile + " context. " +
                        (numberOfRetries == 0 ? "" : "Reattempt in " + timeoutBetweenRetriesInMs + "ms"));
                    try {
                        Thread.sleep(timeoutBetweenRetriesInMs);
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            }
        }

        public void execute(final Callback callback) {
            if (logger.isDebugEnabled()) {
                logger.debug("Executing callback on " + profile);
            }
            TransactionTemplate transactionTemplate = context.getBean(TransactionTemplate.class);
            transactionTemplate.execute(new TransactionCallback<Object>() {

                @Override
                public Object doInTransaction(TransactionStatus status) {
                    SessionFactory sessionFactory = context.getBean(SessionFactory.class);
                    callback.callback(sessionFactory.getCurrentSession());
                    return null;
                }
            });
        }

        public <T> T getBean(Class<T> cls) {
            return context.getBean(cls);
        }

        public void close() {
            context.close();
        }

        public abstract class Callback {

            public abstract void callback(Session session);
        }
    }

}
