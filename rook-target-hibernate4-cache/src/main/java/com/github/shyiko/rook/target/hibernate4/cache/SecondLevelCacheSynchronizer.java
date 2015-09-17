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
import org.hibernate.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class SecondLevelCacheSynchronizer extends AbstractCacheSynchronizer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public SecondLevelCacheSynchronizer(SynchronizationContext synchronizationContext) {
        super(synchronizationContext);
        if (!synchronizationContext.getSessionFactory().getSettings().isSecondLevelCacheEnabled()) {
            throw new IllegalStateException(
                    "Second Level Cache (controlled by hibernate.cache.use_second_level_cache property) is disabled");
        }
    }

    protected void processTX(Collection<RowsMutationReplicationEvent> txEvents) {
        for (RowsMutationReplicationEvent event : txEvents) {
            Cache cache = synchronizationContext.getSessionFactory().getCache();
            String qualifiedName = event.getSchema().toLowerCase() + "." + event.getTable().toLowerCase();

            for (EvictionTarget evictionTarget : synchronizationContext.getEvictionTargets(qualifiedName)) {
                for (Serializable[] row : resolveAffectedRows(event)) {
                    Serializable key = evictionTarget.getPrimaryKey().getIdentifier(row);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Evicting " + evictionTarget.getName() + "#" + key);
                    }
                    // todo(shyiko): do we need a lock here?
                    if (evictionTarget.isCollection()) {
                        if (key == null) {
                            continue; // that's ok, there is no mapped collection for this row
                        }
                        cache.evictCollection(evictionTarget.getName(), key);
                    } else {
                        if (key == null) {
                            throw new IllegalStateException("Failed to extract primary key from " + evictionTarget);
                        }
                        cache.evictEntity(evictionTarget.getName(), key);
                    }
                }
            }
        }
    }
}
