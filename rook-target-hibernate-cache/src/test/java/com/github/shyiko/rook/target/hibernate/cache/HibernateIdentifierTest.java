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

import com.github.shyiko.rook.target.hibernate.cache.model.DummyEntity;
import com.github.shyiko.rook.target.hibernate.cache.model.DummyEntityTwoFieldPK;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTransactionalTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 */
@ContextConfiguration(locations = {
        "classpath:hibernate-spring-test-context.xml"
})
public class HibernateIdentifierTest extends AbstractTransactionalTestNGSpringContextTests {

    @Autowired
    private SessionFactory sessionFactory;

    @Test
    public void testCacheSave() throws Exception {
        Session session = sessionFactory.getCurrentSession();

        DummyEntityTwoFieldPK entity = new DummyEntityTwoFieldPK();
        entity.setId(1L);
        entity.setId2(2L);
        entity.setName("name");
        session.save(entity);
        DummyEntityTwoFieldPK dummyKey = new DummyEntityTwoFieldPK();
        dummyKey.setId(1L);
        dummyKey.setId2(2L);
        Serializable identifier = session.getIdentifier(entity);
        Assert.assertEquals(identifier.getClass(), DummyEntityTwoFieldPK.class);
        Object o = session.get(DummyEntityTwoFieldPK.class, dummyKey);
        Assert.assertNotNull(o);
    }

    @Test
    public void testOnePK() throws Exception {
        Session session = sessionFactory.getCurrentSession();

        DummyEntity entity = new DummyEntity();
        entity.setId(1L);
        entity.setName("name");
        session.save(entity);
        Serializable identifier = session.getIdentifier(entity);
        Assert.assertEquals(identifier.getClass(), Long.class);
    }
}
