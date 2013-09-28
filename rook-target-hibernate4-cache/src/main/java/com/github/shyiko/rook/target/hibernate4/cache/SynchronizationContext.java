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
package com.github.shyiko.rook.target.hibernate4.cache;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.PersistentClass;
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

    private final String schema;
    private final SessionFactory sessionFactory;
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
            String entityName = persistentClass.getEntityName();
            boolean isCacheable = ((SessionFactoryImplementor) sessionFactory).
                getEntityPersister(entityName).hasCache();
            if (isCacheable) {
                Table table = persistentClass.getTable();
                PrimaryKey primaryKey = new PrimaryKey(persistentClass);
                evictionTargetsOf(table).add(new EvictionTarget(entityName, primaryKey, false));
            }
        }
    }

    private void loadCollectionMappings(Configuration configuration) {
        @SuppressWarnings("unchecked")
        Iterator<org.hibernate.mapping.Collection> iterator = configuration.getCollectionMappings();
        while (iterator.hasNext()) {
            org.hibernate.mapping.Collection collection = iterator.next();
            String role = collection.getRole();
            boolean isCacheable = ((SessionFactoryImplementor) sessionFactory).
                getCollectionPersister(role).hasCache();
            if (isCacheable) {
                Table table = collection.getCollectionTable();
                PrimaryKey primaryKey = new PrimaryKey(collection);
                evictionTargetsOf(table).add(new EvictionTarget(role, primaryKey, true));
            }
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
}
