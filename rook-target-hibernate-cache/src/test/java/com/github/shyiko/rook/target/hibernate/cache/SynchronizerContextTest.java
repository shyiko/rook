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
import org.hibernate.SessionFactory;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTransactionalTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 */
@ContextConfiguration(locations = {
        "classpath:hibernate-spring-test-context.xml"
})
public class SynchronizerContextTest extends AbstractTransactionalTestNGSpringContextTests {

    @Autowired
    private SessionFactory sessionFactory;
    @Autowired
    private LocalSessionFactoryBean sessionFactoryBean;

    private String[] mapColumnValues(String entityName, Map<String, String> columnValuesByName) {
        Table table = sessionFactoryBean.getConfiguration().getClassMapping(entityName).getTable();
        List<String> result = new ArrayList<String>();
        for (@SuppressWarnings("unchecked") Iterator<Column> iterator = table.getColumnIterator();
             iterator.hasNext(); ) {
            Column column = iterator.next();
            if (columnValuesByName.containsKey(column.getName())) {
                result.add(columnValuesByName.get(column.getName()));
            }
        }
        return result.toArray(new String[result.size()]);
    }

    @Test
    public void testKeyMappings() throws Exception {
        SynchronizationContext synchronizationContext = new SynchronizationContext(
                sessionFactoryBean.getConfiguration(), sessionFactory);

        List<EvictionTarget> dummyEvictionTargets = new ArrayList<EvictionTarget>(synchronizationContext.
                getEvictionTargets("rook.dummy_entity"));
        String[] allFieldsFofDummy = mapColumnValues(DummyEntity.class.getName(),
                new LinkedHashMap<String, String>() {
                    {
                        put("id", "id");
                        put("name", "name");
                    }
                });
        String[] keyFieldsFofDummy = mapColumnValues(DummyEntity.class.getName(),
                new LinkedHashMap<String, String>() {
                    {
                        put("id", "id");
                    }
                });
        Assert.assertEquals(dummyEvictionTargets.get(0).getPrimaryKey().of(allFieldsFofDummy), keyFieldsFofDummy);
    }

    @Test
    public void testKeyMappingsForDummyTwoPk() throws Exception {
        SynchronizationContext synchronizationContext = new SynchronizationContext(
                sessionFactoryBean.getConfiguration(), sessionFactory);

        List<EvictionTarget> dummyEvictionTargets = new ArrayList<EvictionTarget>(synchronizationContext.
                getEvictionTargets("rook.dummy_entity_2fpk"));
        String[] allFieldsFofDummy2 = mapColumnValues(DummyEntityTwoFieldPK.class.getName(),
                new LinkedHashMap<String, String>() {
                    {
                        put("id", "id");
                        put("id2", "id2");
                        put("name", "name");
                    }
                });

        String[] keyFieldsFofDummy2 = mapColumnValues(DummyEntityTwoFieldPK.class.getName(),
                new LinkedHashMap<String, String>() {
                    {
                        put("id", "id");
                        put("id2", "id2");
                    }
                });

        Assert.assertEquals(dummyEvictionTargets.get(0).getPrimaryKey().of(allFieldsFofDummy2), keyFieldsFofDummy2);
    }

}
