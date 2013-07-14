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

import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.event.CompositeReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.RowReplicationEvent;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class QueryCacheSynchronizer implements ReplicationEventListener {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SynchronizationContext synchronizationContext;

    public QueryCacheSynchronizer(SynchronizationContext synchronizationContext) {
        if (!synchronizationContext.getSessionFactory().getSettings().isQueryCacheEnabled()) {
            throw new IllegalStateException(
                    "Query Cache (controlled by hibernate.cache.use_query_cache property) is disabled");
        }
        this.synchronizationContext = synchronizationContext;
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        Collection<RowReplicationEvent> events = null;
        if (event instanceof CompositeReplicationEvent) {
            Collection<ReplicationEvent> replicationEvents = ((CompositeReplicationEvent) event).getEvents();
            events = new ArrayList<RowReplicationEvent>(replicationEvents.size());
            for (ReplicationEvent replicationEvent : replicationEvents) {
                if (replicationEvent instanceof RowReplicationEvent) {
                    events.add((RowReplicationEvent) replicationEvent);
                }
            }
        } else if (event instanceof RowReplicationEvent) {
            events = new LinkedList<RowReplicationEvent>();
            events.add((RowReplicationEvent) event);
        }
        if (events != null && !events.isEmpty()) {
            invalidateAffectedQuerySpaces(events);
        }
    }

    private void invalidateAffectedQuerySpaces(Collection<RowReplicationEvent> events) {
        Set<String> spacesToInvalidate = new HashSet<String>();
        for (RowReplicationEvent event : events) {
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
