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

import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.event.DeleteRowsReplicationEvent;
import com.github.shyiko.rook.api.event.InsertRowsReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.RowsMutationReplicationEvent;
import com.github.shyiko.rook.api.event.TXReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowsReplicationEvent;
import org.hibernate.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class SecondLevelCacheSynchronizer implements ReplicationEventListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SynchronizationContext synchronizationContext;

    public SecondLevelCacheSynchronizer(SynchronizationContext synchronizationContext) {
        if (!synchronizationContext.getSessionFactory().getSettings().isSecondLevelCacheEnabled()) {
            throw new IllegalStateException(
                "Second Level Cache (controlled by hibernate.cache.use_second_level_cache property) is disabled");
        }
        this.synchronizationContext = synchronizationContext;
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        if (event instanceof TXReplicationEvent) {
            for (ReplicationEvent replicationEvent : ((TXReplicationEvent) event).getEvents()) {
                evict((RowsMutationReplicationEvent) replicationEvent);
            }
        } else
        if (event instanceof RowsMutationReplicationEvent) {
            evict((RowsMutationReplicationEvent) event);
        }
    }

    private void evict(RowsMutationReplicationEvent event) {
        Cache cache = synchronizationContext.getSessionFactory().getCache();
        String qualifiedName = event.getSchema().toLowerCase() + "." + event.getTable().toLowerCase();
        Collection<EvictionTarget> evictionTargets = synchronizationContext.getEvictionTargets(qualifiedName);
        for (Serializable[] row : resolveAffectedRows(event)) {
            for (EvictionTarget evictionTarget : evictionTargets) {
                Serializable key = evictionTarget.getPrimaryKey().getIdentifier(row);
                if (logger.isDebugEnabled()) {
                    logger.debug("Evicting " + evictionTarget.getName() + "#" + key);
                }
                // todo(shyiko): do we need a lock here?
                if (evictionTarget.isCollection()) {
                    cache.evictCollection(evictionTarget.getName(), key);
                } else {
                    cache.evictEntity(evictionTarget.getName(), key);
                }
            }
        }
    }

    private List<Serializable[]> resolveAffectedRows(RowsMutationReplicationEvent event) {
        if (event instanceof InsertRowsReplicationEvent) {
            return ((InsertRowsReplicationEvent) event).getRows();
        }
        if (event instanceof UpdateRowsReplicationEvent) {
            List<Map.Entry<Serializable[], Serializable[]>> rows = ((UpdateRowsReplicationEvent) event).getRows();
            List<Serializable[]> result = new ArrayList<Serializable[]>(rows.size());
            for (Map.Entry<Serializable[], Serializable[]> row : rows) {
                result.add(row.getKey());
            }
            return result;
        }
        if (event instanceof DeleteRowsReplicationEvent) {
            return ((DeleteRowsReplicationEvent) event).getRows();
        }
        throw new UnsupportedOperationException("Unexpected " + event.getClass());
    }

}
