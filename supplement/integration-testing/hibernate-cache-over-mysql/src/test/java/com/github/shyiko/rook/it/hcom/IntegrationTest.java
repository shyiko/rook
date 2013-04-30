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
import com.github.shyiko.rook.api.ReplicationStream;
import com.github.shyiko.rook.api.event.InsertRowReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowReplicationEvent;
import com.github.shyiko.rook.it.hcom.model.OneToManyEntity;
import com.github.shyiko.rook.it.hcom.model.OneToOneEntity;
import com.github.shyiko.rook.it.hcom.model.RootEntity;
import com.github.shyiko.rook.source.mysql.MySQLReplicationStream;
import com.github.shyiko.rook.target.hibernate.cache.QueryCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate.cache.SecondLevelCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate.cache.mapping.EvictionTargetRegistry;
import net.sf.ehcache.CacheManager;
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
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class IntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);
    private ReplicationStream replicationStream;

    @BeforeClass
    public void setUp() throws Exception {
        ResourceBundle bundle = ResourceBundle.getBundle("slave");
        String dsURL = bundle.getString("datasource.url");
        URI uri = new URI("schema" + dsURL.substring(dsURL.indexOf("://")));
        replicationStream = new MySQLReplicationStream(uri.getHost(), uri.getPort()).
            usingCredentials(bundle.getString("datasource.username"), bundle.getString("datasource.password"));
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
            replicationStream.registerListener(
                new QueryCacheSynchronizer(slaveContext.getBean(SessionFactory.class))
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

    @Test()
    public void testSecondLevelCacheIsNotEvictedWithoutSLCS() throws Exception {
        testSecondLevelCacheEviction(false);
    }

    @Test()
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
                new SecondLevelCacheSynchronizer(slaveContext.getBean(SessionFactory.class),
                    new EvictionTargetRegistry(configuration))
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

    private static class ExecutionContext {

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
                    // failure of context.refresh leaves alive CacheManager instance
                    // shutting it down before reattempting
                    // todo: is this really needed when there is HibernateCacheManagerShutdownInResponseToCCE
                    CacheManager cacheManager = CacheManager.getCacheManager(profile + "-ehcache");
                    if (cacheManager != null) {
                        cacheManager.shutdown();
                    }
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
