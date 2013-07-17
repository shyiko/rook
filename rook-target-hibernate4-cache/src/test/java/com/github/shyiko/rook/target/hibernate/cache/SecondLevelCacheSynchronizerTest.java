/*
 * Copyright 2013 Ivan Zaytsev
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
package com.github.shyiko.rook.target.hibernate.cache;

import com.github.shyiko.rook.api.event.DeleteRowReplicationEvent;
import com.github.shyiko.rook.target.hibernate.cache.model.EntityWithCompositeKey;
import com.github.shyiko.rook.target.hibernate4.cache.SecondLevelCacheSynchronizer;
import com.github.shyiko.rook.target.hibernate4.cache.SynchronizationContext;
import org.hibernate.Cache;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 */
public class SecondLevelCacheSynchronizerTest {

    private SynchronizationContext synchronizationContext;

    @BeforeClass
    public void setUp() {
        Configuration configuration = new Configuration().configure("hibernate.cfg.xml");
        configuration.setProperty("hibernate.cache.region.factory_class",
                "org.hibernate.cache.ehcache.EhCacheRegionFactory");
        configuration.setProperty("hibernate.cache.use_second_level_cache", "true");
        configuration.setProperty("hibernate.cache.use_query_cache", "true");
        ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().
                applySettings(configuration.getProperties()).buildServiceRegistry();
        SessionFactory sessionFactory = configuration.buildSessionFactory(serviceRegistry);
        synchronizationContext = new SynchronizationContext(configuration, sessionFactory);
    }

    @Test
    public void testEvictionOfEntityWithCompositeKey() throws Exception {
        final Cache cache = synchronizationContext.getSessionFactory().getCache();
        final EntityWithCompositeKey firstEntityKey = new EntityWithCompositeKey(1, 2),
                secondEntityKey = new EntityWithCompositeKey(3, 4);
        executeInTransaction(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.save(new EntityWithCompositeKey(1, 2, "name_12"));
                session.save(new EntityWithCompositeKey(3, 4, "name_34"));
            }
        });
        executeInTransaction(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                session.get(EntityWithCompositeKey.class, firstEntityKey);
                session.get(EntityWithCompositeKey.class, secondEntityKey);
            }
        });
        executeInTransaction(new Callback<Session>() {

            @Override
            public void execute(Session obj) {
                assertTrue(cache.containsEntity(EntityWithCompositeKey.class, firstEntityKey));
                SecondLevelCacheSynchronizer secondLevelCacheSynchronizer =
                        new SecondLevelCacheSynchronizer(synchronizationContext);
                secondLevelCacheSynchronizer.onEvent(new DeleteRowReplicationEvent("rook", "entity_with_cpk",
                        new Serializable[] {2L, 1L}));
                assertFalse(cache.containsEntity(EntityWithCompositeKey.class, firstEntityKey));
                assertTrue(cache.containsEntity(EntityWithCompositeKey.class, secondEntityKey));
            }
        });
    }

    private void executeInTransaction(Callback<Session> callback) {
        SessionFactory sessionFactory = synchronizationContext.getSessionFactory();
        Session session = sessionFactory.openSession();
        try {
            session.beginTransaction();
            callback.execute(session);
            session.getTransaction().commit();
        } finally {
            session.close();
        }
    }

    @AfterClass
    public void tearDown() {
        synchronizationContext.getSessionFactory().close();
    }

    private interface Callback<T> {

        void execute(T obj);
    }

}
