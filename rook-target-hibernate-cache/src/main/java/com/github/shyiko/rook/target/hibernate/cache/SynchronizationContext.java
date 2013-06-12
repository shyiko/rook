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
package com.github.shyiko.rook.target.hibernate.cache;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.KeyValue;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Selectable;
import org.hibernate.mapping.Table;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class SynchronizationContext {

    private String schema;
    private SessionFactory sessionFactory;
    private final Map<String, Collection<EvictionTarget>> targetsByTable =
            new HashMap<String, Collection<EvictionTarget>>();

    public SynchronizationContext(Configuration configuration, SessionFactory sessionFactory) {
        this.schema = ((SessionFactoryImplementor) sessionFactory).getJdbcServices().
                getExtractedMetaDataSupport().getConnectionCatalogName().toLowerCase();
        this.sessionFactory = sessionFactory;
        loadClassMappings(configuration);
        loadCollectionMappings(configuration);
    }

    public SessionFactoryImplementor getSessionFactory() {
        return (SessionFactoryImplementor) sessionFactory;
    }

    public Collection<EvictionTarget> getEvictionTargets(String table) {
        Collection<EvictionTarget> evictionTargets = targetsByTable.get(table.toLowerCase());
        return evictionTargets == null ? Collections.<EvictionTarget>emptyList() : evictionTargets;
    }

    private void loadClassMappings(Configuration configuration) {
        for (Iterator<PersistentClass> iterator = configuration.getClassMappings(); iterator.hasNext(); ) {
            PersistentClass persistentClass = iterator.next();
            Table table = persistentClass.getTable();
            String className = persistentClass.getClassName();
            Map<String, Integer> columnIndexByName = asIndexMap(table);
            PrimaryKey primaryKey = asPrimaryKey(persistentClass.getKey(), columnIndexByName);
            evictionTargetsOf(table).add(new EvictionTarget(className, primaryKey, false));
        }
    }

    private void loadCollectionMappings(Configuration configuration) {
        for (@SuppressWarnings("unchecked") Iterator<org.hibernate.mapping.Collection> iterator =
                     configuration.getCollectionMappings();
             iterator.hasNext(); ) {
            org.hibernate.mapping.Collection collection = iterator.next();
            Table table = collection.getCollectionTable();
            String role = collection.getRole();
            Map<String, Integer> columnIndexByName = asIndexMap(table);
            PrimaryKey primaryKey = asPrimaryKey(collection.getKey(), columnIndexByName);
            evictionTargetsOf(table).add(new EvictionTarget(role, primaryKey, true));
        }
    }

    private Collection<EvictionTarget> evictionTargetsOf(Table table) {
        String key = schema + "." + table.getName().toLowerCase();
        Collection<EvictionTarget> evictionTargets = targetsByTable.get(key);
        if (evictionTargets == null) {
            targetsByTable.put(key, evictionTargets = new LinkedList<EvictionTarget>());
        }
        return evictionTargets;
    }

    private Map<String, Integer> asIndexMap(Table table) {
        Map<String, Integer> columnIndexByName = new HashMap<String, Integer>();
        int index = 0;
        for (@SuppressWarnings("unchecked") Iterator<Column> columnIterator = table.getColumnIterator();
             columnIterator.hasNext(); ) {
            Column column = columnIterator.next();
            columnIndexByName.put(column.getName(), index++);
        }
        return columnIndexByName;
    }

    @SuppressWarnings("unchecked")
    private PrimaryKey asPrimaryKey(KeyValue keyValue, Map<String, Integer> indexMap) {
        int[] pkIndexes = new int[keyValue.getColumnSpan()];
        int index = 0;
        for (Iterator<Selectable> columnIterator = keyValue.getColumnIterator();
             columnIterator.hasNext(); ) {
            Column column = (Column) columnIterator.next(); // todo: what if it's Formula?
            pkIndexes[index++] = indexMap.get(column.getName());
        }
        return new PrimaryKey(pkIndexes);
    }

}
