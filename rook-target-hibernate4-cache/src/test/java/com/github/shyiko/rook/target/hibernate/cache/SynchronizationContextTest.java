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

import com.github.shyiko.rook.target.hibernate.cache.model.EntityWithCompositeKey;
import com.github.shyiko.rook.target.hibernate4.cache.PrimaryKey;
import com.github.shyiko.rook.target.hibernate4.cache.SynchronizationContext;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 */
public class SynchronizationContextTest {

    private SynchronizationContext synchronizationContext;

    @BeforeClass
    public void setUp() {
        Configuration configuration = new Configuration().configure("hibernate.cfg.xml");
        ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().
                applySettings(configuration.getProperties()).buildServiceRegistry();
        SessionFactory sessionFactory = configuration.buildSessionFactory(serviceRegistry);
        synchronizationContext = new SynchronizationContext(configuration, sessionFactory);
    }

    @Test
    public void testKeyMapping() throws Exception {
        PrimaryKey primaryKey = synchronizationContext.getEvictionTargets("rook.entity").
                iterator().next().getPrimaryKey();
        Serializable[] allFieldsFofDummy = new Serializable[] {1L, "name"};
        Assert.assertEquals(primaryKey.getIdentifier(allFieldsFofDummy), (Long) 1L);
    }

    @Test
    public void testCompositeKeyMapping() throws Exception {
        PrimaryKey primaryKey = synchronizationContext.getEvictionTargets("rook.entity_with_cpk").
                iterator().next().getPrimaryKey();
        EntityWithCompositeKey expectedKey = new EntityWithCompositeKey(1L, 2L);
        Assert.assertTrue(EqualsBuilder.reflectionEquals(
                primaryKey.getIdentifier(new Serializable[]{2L, 1L, "name"}), expectedKey));
    }

    @AfterClass
    public void tearDown() {
        synchronizationContext.getSessionFactory().close();
    }

}
