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

import com.github.shyiko.rook.api.ReplicationListener;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.RowReplicationEvent;
import com.github.shyiko.rook.target.hibernate.cache.mapping.EvictionTarget;
import com.github.shyiko.rook.target.hibernate.cache.mapping.EvictionTargetRegistry;
import org.hibernate.Cache;
import org.hibernate.SessionFactory;

import java.io.Serializable;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class SecondLevelCacheSynchronizer implements ReplicationListener {

    private SessionFactory sessionFactory;
    private EvictionTargetRegistry evictionTargetRegistry;

    public SecondLevelCacheSynchronizer(SessionFactory sessionFactory, EvictionTargetRegistry evictionTargetRegistry) {
        this.sessionFactory = sessionFactory;
        this.evictionTargetRegistry = evictionTargetRegistry;
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        if (!(event instanceof RowReplicationEvent)) {
            return;
        }
        RowReplicationEvent rowEvent = (RowReplicationEvent) event;
        String tableName = rowEvent.getTable();
        Cache cache = sessionFactory.getCache();
        for (EvictionTarget evictionTarget : evictionTargetRegistry.getEvictionTargets(tableName)) {
            Serializable[] id = evictionTarget.getPrimaryKey().of(rowEvent.getValues());
            if (id.length != 1) {
                throw new UnsupportedOperationException(); // todo: yet to implement
            }
            Serializable key = id[0];
            if (evictionTarget.isCollection()) {
                cache.evictCollection(evictionTarget.getName(), key);
            } else {
                cache.evictEntity(evictionTarget.getName(), key);
            }
        }
    }
}
