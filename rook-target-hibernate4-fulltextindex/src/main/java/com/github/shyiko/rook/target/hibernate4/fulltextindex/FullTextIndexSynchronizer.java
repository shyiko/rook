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
package com.github.shyiko.rook.target.hibernate4.fulltextindex;

import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.event.CompositeReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.RowReplicationEvent;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Table;
import org.hibernate.search.annotations.Indexed;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class FullTextIndexSynchronizer implements ReplicationEventListener {

    private final String schema;
    private final EntityIndexer entityIndexer;
    private final Map<String, Collection<EvictionTarget>> targetsByTable =
        new HashMap<String, Collection<EvictionTarget>>();

    public FullTextIndexSynchronizer(Configuration configuration, SessionFactory sessionFactory) {
        this(configuration, sessionFactory, new DefaultEntityIndexer(sessionFactory));
    }

    public FullTextIndexSynchronizer(Configuration configuration, SessionFactory sessionFactory,
            EntityIndexer entityIndexer) {
        this.schema = ((SessionFactoryImplementor) sessionFactory).getJdbcServices().
            getExtractedMetaDataSupport().getConnectionCatalogName().toLowerCase();
        this.entityIndexer = entityIndexer;
        loadClassMappings(configuration);
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        // todo: index on transaction commit (as per binlog)
        if (event instanceof CompositeReplicationEvent) {
            for (ReplicationEvent replicationEvent : ((CompositeReplicationEvent) event).getEvents()) {
                updateIndex((RowReplicationEvent) replicationEvent);
            }
        } else
        if (event instanceof RowReplicationEvent) {
            updateIndex((RowReplicationEvent) event);
        }
    }

    private void loadClassMappings(Configuration configuration) {
        for (Iterator<PersistentClass> iterator = configuration.getClassMappings(); iterator.hasNext(); ) {
            PersistentClass persistentClass = iterator.next();
            if (!persistentClass.getMappedClass().isAnnotationPresent(Indexed.class)) {
                continue;
            }
            Table table = persistentClass.getTable();
            String className = persistentClass.getClassName();
            PrimaryKey primaryKey = new PrimaryKey(persistentClass);
            evictionTargetsOf(table).add(new EvictionTarget(className, primaryKey, false));
        }
    }

    private void updateIndex(RowReplicationEvent event) {
        String qualifiedName = event.getSchema().toLowerCase() + "." + event.getTable().toLowerCase();
        Collection<EvictionTarget> evictionTargets = getEvictionTargets(qualifiedName);
        for (EvictionTarget evictionTarget : evictionTargets) {
            PrimaryKey primaryKey = evictionTarget.getPrimaryKey();
            Class entityClass = primaryKey.getEntityClass();
            Serializable id = primaryKey.getIdentifier(event.getValues());
            entityIndexer.index(entityClass, id);
        }
    }

    // todo: refactor out
    private Collection<EvictionTarget> getEvictionTargets(String table) {
        Collection<EvictionTarget> evictionTargets = targetsByTable.get(table.toLowerCase());
        return evictionTargets == null ? Collections.<EvictionTarget>emptyList() : evictionTargets;
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
