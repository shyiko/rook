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
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class HibernateCacheSynchronizer implements ReplicationListener {

    private final List<ReplicationListener> listeners;

    public HibernateCacheSynchronizer(Configuration configuration, SessionFactory sessionFactory) {
        listeners = new ArrayList<ReplicationListener>();
        SynchronizationContext synchronizationContext = new SynchronizationContext(configuration, sessionFactory);
        listeners.add(new SecondLevelCacheSynchronizer(synchronizationContext));
        if (synchronizationContext.getSessionFactory().getSettings().isQueryCacheEnabled()) {
            listeners.add(new QueryCacheSynchronizer(synchronizationContext));
        }
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        for (ReplicationListener listener : listeners) {
            listener.onEvent(event);
        }
    }

}
