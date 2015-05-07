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
import com.github.shyiko.rook.target.hibernate4.cache.EvictionTarget;
import com.github.shyiko.rook.target.hibernate4.cache.PrimaryKey;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * @author <a href="mailto:ivan.zaytsev@webamg.com">Ivan Zaytsev</a>
 */
public class SynchronizationContextTest extends AbstractHibernateTest {

    @Test
    public void testSimpleKeyMapping() throws Exception {
        PrimaryKey primaryKey = synchronizationContext.getEvictionTargets("rook.entity").
                iterator().next().getPrimaryKey();
        Serializable[] allFieldsFofDummy = new Serializable[] {1L, "name"};
        assertEquals(primaryKey.getIdentifier(allFieldsFofDummy), (Long) 1L);
    }

    @Test
    public void testCollectionMapping() throws Exception {
        Collection<EvictionTarget> evictionTargets = synchronizationContext.getEvictionTargets("rook.entity_property");
        assertEquals(1, evictionTargets.size());
        EvictionTarget collectionEvictionTarget = evictionTargets.iterator().next();

        // simulating correct binlog column order
        Map<String, Integer> mappingsByName = synchronizationContext.getColumnMappingsByTable("entity_property");
        Serializable[] collectionfields = new Serializable[mappingsByName.size()];
        collectionfields[mappingsByName.get("id")] = 2L;
        collectionfields[mappingsByName.get("entity_id")] = 1L;
        collectionfields[mappingsByName.get("name")] = "Answer to the Ultimate Question of Life, the Universe, and Everything";
        collectionfields[mappingsByName.get("value")] = "42";

        assertEquals(collectionEvictionTarget.getPrimaryKey().getIdentifier(collectionfields), (Long) 1L);
    }

    @Test
    public void testCompositeKeyMapping() throws Exception {
        PrimaryKey primaryKey = synchronizationContext.getEvictionTargets("rook.entity_with_cpk").
                iterator().next().getPrimaryKey();
        EntityWithCompositeKey expectedKey = new EntityWithCompositeKey(1L, 2L);
        Assert.assertTrue(EqualsBuilder.reflectionEquals(
                primaryKey.getIdentifier(new Serializable[]{2L, 1L, "name"}), expectedKey));
    }

    @Test
    public void testNonCacheableEntityMapping() throws Exception {
        Assert.assertTrue(synchronizationContext.getEvictionTargets("rook.non_cacheable_entity").isEmpty());
    }

}
