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
import com.github.shyiko.rook.target.hibernate.cache.model.DummyEntityTwoFieldPK;
import org.hibernate.Cache;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertFalse;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 */
@ContextConfiguration(locations = {
        "classpath:hibernate-ehcache-spring-test-context.xml"
})
public class SecondLevelSynchronizerEvictionTest extends AbstractTestNGSpringContextTests {

    @Autowired
    private SessionFactory sessionFactory;
    @Autowired
    private LocalSessionFactoryBean sessionFactoryBean;

    @Autowired
    private HibernateTransactionManager transactionManager;
    private SecondLevelCacheSynchronizer secondLevelCacheSynchronizer;
    private Serializable[] values;
    private Cache cache;
    private Serializable identifier;

    private void init() {
        SynchronizationContext synchronizationContext = new SynchronizationContext(
                sessionFactoryBean.getConfiguration(), sessionFactory);

        secondLevelCacheSynchronizer = new SecondLevelCacheSynchronizer(synchronizationContext);

        values = mapColumnValues(sessionFactoryBean.getConfiguration(),
                DummyEntityTwoFieldPK.class.getName(), new HashMap<String, Serializable>() {
            {
                put("id", 1L);
                put("id2", 2L);
            }
        });

        cache = synchronizationContext.getSessionFactory().getCache();

        identifier = new Serializable[1];
    }

    private Serializable[] mapColumnValues(Configuration configuration, String entityName,
            Map<String, Serializable> columnValuesByName) {
        Table table = configuration.getClassMapping(entityName).getTable();
        List<Serializable> result = new ArrayList<Serializable>();
        for (@SuppressWarnings("unchecked") Iterator<Column> iterator = table.getColumnIterator();
             iterator.hasNext(); ) {
            Column column = iterator.next();
            if (columnValuesByName.containsKey(column.getName())) {
                result.add(columnValuesByName.get(column.getName()));
            }
        }
        return result.toArray(new Serializable[result.size()]);
    }

    @Test
    public void testKeyMappingsForDummyTwoPk() throws Exception {
        init();

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                DummyEntityTwoFieldPK entity = new DummyEntityTwoFieldPK();
                entity.setId(1L);
                entity.setId2(2L);
                entity.setName("name");

                final Session session = sessionFactory.getCurrentSession();
                session.save(entity);

                identifier = session.getIdentifier(entity);

                session.flush();
                session.clear();
            }
        });

        new TransactionTemplate(transactionManager).execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                final Session session = sessionFactory.getCurrentSession();
                Object entity1 = session.get(DummyEntityTwoFieldPK.class, identifier);
                Assert.assertNotNull(entity1);
            }
        });

        Assert.assertTrue(cache.containsEntity(DummyEntityTwoFieldPK.class, identifier));

        new TransactionTemplate(transactionManager).execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                final Session session = sessionFactory.getCurrentSession();
                Object entity1 = session.get(DummyEntityTwoFieldPK.class, identifier);
                Assert.assertNotNull(entity1);
            }
        });

        secondLevelCacheSynchronizer.onEvent(new DeleteRowReplicationEvent("rook", "dummy_entity_2fpk", values));

        assertFalse(cache.containsEntity(DummyEntityTwoFieldPK.class, identifier));
    }

}
