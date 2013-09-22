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

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class DefaultRowsMutationIndexer implements RowsMutationIndexer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void index(List<RowsMutation> rowsMutations, SynchronizationContext synchronizationContext) {
        IndexingLog indexingLog = new IndexingLog();
        Session session = synchronizationContext.getSessionFactory().openSession();
        try {
            FullTextSession fullTextSession = Search.getFullTextSession(session);
            Transaction tx = fullTextSession.beginTransaction();
            try {
                for (RowsMutation entity : rowsMutations) {
                    index(fullTextSession, entity, indexingLog, synchronizationContext);
                }
                tx.commit();
            } catch (RuntimeException e) {
                tx.rollback();
            }
        } finally {
            session.close();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Indexed " + indexingLog.toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void index(FullTextSession session, RowsMutation rowsMutation, IndexingLog indexingLog,
            SynchronizationContext synchronizationContext) {
        IndexingDirective indexingDirective = rowsMutation.getIndexingDirective();
        PrimaryKey primaryKey = indexingDirective.getPrimaryKey();
        Class entityClass = primaryKey.getEntityClass();
        for (Serializable[] row : rowsMutation.getRows()) {
            Serializable id = primaryKey.getIdentifier(row);
            if (indexingLog.isIndexed(entityClass, id)) {
                continue;
            }
            Object entity = loadEntity(session, entityClass, id);
            if (!indexingDirective.isSuppressSelfIndexing()) {
                if (entity != null) {
                    session.index(entity);
                } else {
                    session.purge(entityClass, id);
                }
            }
            indexingLog.markIndexed(entityClass, id);
            if (entity != null) {
                indexContainers(session, entity, indexingDirective, indexingLog, synchronizationContext);
            }
        }
    }

    private void indexContainers(FullTextSession session, Object entity, IndexingDirective indexingDirective,
            IndexingLog indexingLog, SynchronizationContext synchronizationContext) {
        for (Reference containerReference : indexingDirective.getContainerReferences()) {
            Object container = containerReference.navigateFrom(entity);
            if (container != null) {
                IndexingDirective containerIndexingDirective =
                    synchronizationContext.getIndexingDirective(containerReference.getTargetEntityClass());
                if (container instanceof Collection) {
                    for (Object containerEntity : (Collection) container) {
                        indexContainer(session, containerEntity, containerIndexingDirective, indexingLog,
                            synchronizationContext);
                    }
                } else {
                    indexContainer(session, container, containerIndexingDirective, indexingLog,
                        synchronizationContext);
                }
            }
        }
    }

    private void indexContainer(FullTextSession session, Object entity, IndexingDirective indexingDirective,
            IndexingLog indexingLog, SynchronizationContext synchronizationContext) {
        PrimaryKey primaryKey = indexingDirective.getPrimaryKey();
        Class entityClass = primaryKey.getEntityClass();
        Serializable id = primaryKey.getIdentifier(entity);
        if (indexingLog.isIndexed(entityClass, id)) {
            return;
        }
        if (!indexingDirective.isSuppressSelfIndexing()) {
            session.index(entity);
        }
        indexingLog.markIndexed(entityClass, id);
        indexContainers(session, entity, indexingDirective, indexingLog, synchronizationContext);
    }

    protected Object loadEntity(Session session, Class entityClass, Serializable id) {
        return session.get(entityClass, id);
    }

    private static class IndexingLog {

        private Map<Class, Collection<Serializable>> indexedEntities = new HashMap<Class, Collection<Serializable>>();

        public void markIndexed(Class entityClass, Serializable id) {
            Collection<Serializable> ids = indexedEntities.get(entityClass);
            if (ids == null) {
                indexedEntities.put(entityClass, ids = new TreeSet<Serializable>());
            }
            ids.add(id);
        }

        public boolean isIndexed(Class entityClass, Serializable id) {
            Collection<Serializable> ids = indexedEntities.get(entityClass);
            return ids != null && ids.contains(id);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("[");
            if (!indexedEntities.isEmpty()) {
                for (Map.Entry<Class, Collection<Serializable>> entry : indexedEntities.entrySet()) {
                    sb.append(entry.getKey().getSimpleName()).append("#").append(entry.getValue()).append(", ");
                }
                sb.replace(sb.length() - 2, sb.length(), "");
            }
            sb.append("]");
            return sb.toString();
        }
    }

}
