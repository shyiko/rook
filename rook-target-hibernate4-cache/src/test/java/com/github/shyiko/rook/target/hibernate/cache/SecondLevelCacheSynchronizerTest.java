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

import com.github.shyiko.rook.api.event.DeleteRowsReplicationEvent;
import com.github.shyiko.rook.target.hibernate.cache.model.Entity;
import com.github.shyiko.rook.target.hibernate.cache.model.EntityProperty;
import com.github.shyiko.rook.target.hibernate.cache.model.EntityWithCompositeKey;
import com.github.shyiko.rook.target.hibernate4.cache.SecondLevelCacheSynchronizer;
import org.hibernate.Cache;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 */
public class SecondLevelCacheSynchronizerTest extends AbstractHibernateTest {

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
                secondLevelCacheSynchronizer.onEvent(new DeleteRowsReplicationEvent("rook", "entity_with_cpk",
                        new Serializable[] {2L, 1L}));
                assertFalse(cache.containsEntity(EntityWithCompositeKey.class, firstEntityKey));
                assertTrue(cache.containsEntity(EntityWithCompositeKey.class, secondEntityKey));
            }
        });
    }

    @Test
    public void testEvictionOfEntityCollection() throws Exception {
        final Cache cache = synchronizationContext.getSessionFactory().getCache();
        final AtomicLong idHolder = new AtomicLong();

        executeInTransaction(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                Entity entity = new Entity();
                entity.setName("Name");

                EntityProperty entityProperty = new EntityProperty();
                entityProperty.setName("name");
                entityProperty.setValue("value");
                entityProperty.setEnclosingEntity(entity);
                entity.getProperties().add(entityProperty);

                idHolder.set((Long) session.save(entity));
            }
        });
        final long ENTITY_ID = idHolder.get();

        executeInTransaction(new Callback<Session>() {

            @Override
            public void execute(Session session) {
                assertNotNull(session.get(Entity.class, ENTITY_ID));
            }
        });

        executeInTransaction(new Callback<Session>() {

            @Override
            public void execute(Session obj) {
                assertTrue(cache.containsEntity(Entity.class, ENTITY_ID));
                SecondLevelCacheSynchronizer secondLevelCacheSynchronizer =
                        new SecondLevelCacheSynchronizer(synchronizationContext);
                secondLevelCacheSynchronizer.onEvent(new DeleteRowsReplicationEvent("rook", "entity",
                        new Serializable[] {ENTITY_ID}));
                assertFalse(cache.containsEntity(Entity.class, ENTITY_ID));

                assertTrue(cache.containsCollection(Entity.class.getName() + ".properties", ENTITY_ID));

                // entity_property table structure [id, name, value, entity_id]
                secondLevelCacheSynchronizer.onEvent(new DeleteRowsReplicationEvent("rook", "entity_property",
                        new Serializable[]{null, null, null, ENTITY_ID}));

                assertFalse(cache.containsCollection(Entity.class.getName() + ".properties", ENTITY_ID));
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

    private interface Callback<T> {

        void execute(T obj);
    }

}
