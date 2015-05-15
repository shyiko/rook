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

import com.github.shyiko.rook.api.event.RowsMutationReplicationEvent;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class QueryCacheSynchronizer extends AbstractCacheSynchronizer {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public QueryCacheSynchronizer(SynchronizationContext synchronizationContext) {
        super(synchronizationContext);
        if (!synchronizationContext.getSessionFactory().getSettings().isQueryCacheEnabled()) {
            throw new IllegalStateException(
                    "Query Cache (controlled by hibernate.cache.use_query_cache property) is disabled");
        }
    }

    @Override
    protected void processSynchronization(Collection<RowsMutationReplicationEvent> events) {
        Set<String> spacesToInvalidate = new HashSet<String>();
        for (RowsMutationReplicationEvent event : events) {
            Collection<EvictionTarget> evictionTargets = synchronizationContext.getEvictionTargets(
                    event.getSchema().toLowerCase() + "." + event.getTable().toLowerCase());
            for (EvictionTarget evictionTarget : evictionTargets) {
                Collections.addAll(spacesToInvalidate, resolveQuerySpaces(evictionTarget));
            }
        }
        if (!spacesToInvalidate.isEmpty()) {
            SessionFactoryImplementor factory = synchronizationContext.getSessionFactory();
            if (logger.isDebugEnabled()) {
                logger.debug("Invalidating spaces: " + spacesToInvalidate);
            }
            factory.getUpdateTimestampsCache().invalidate(spacesToInvalidate.toArray(
                    new Serializable[spacesToInvalidate.size()]));
        }
    }

    private String[] resolveQuerySpaces(EvictionTarget evictionTarget) {
        String role = evictionTarget.getName();
        SessionFactoryImplementor factory = synchronizationContext.getSessionFactory();
        Serializable[] spaces;
        if (evictionTarget.isCollection()) {
            spaces = factory.getCollectionPersister(role).getCollectionSpaces();
        } else {
            // todo(shyiko): how about querySpaces?
            spaces = factory.getEntityPersister(role).getPropertySpaces();
        }
        return spaces == null ? EMPTY_STRING_ARRAY : (String[]) spaces;
    }

}
