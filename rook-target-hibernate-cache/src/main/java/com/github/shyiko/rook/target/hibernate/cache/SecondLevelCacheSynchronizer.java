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
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class SecondLevelCacheSynchronizer implements ReplicationListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private SessionFactory sessionFactory;

    public SecondLevelCacheSynchronizer(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        if (!(event instanceof RowReplicationEvent)) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Cleaning 2nd Level Cache in response to " + event);
        }
        sessionFactory.getCache().evictEntityRegions();
    }
}
