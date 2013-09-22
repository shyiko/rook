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
import com.github.shyiko.rook.api.event.DeleteRowsReplicationEvent;
import com.github.shyiko.rook.api.event.InsertRowsReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.RowsMutationReplicationEvent;
import com.github.shyiko.rook.api.event.TXReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowsReplicationEvent;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class FullTextIndexSynchronizer implements ReplicationEventListener {

    private final SynchronizationContext synchronizationContext;
    private final RowsMutationIndexer indexer;

    public FullTextIndexSynchronizer(Configuration configuration, SessionFactory sessionFactory) {
        this(configuration, sessionFactory, new DefaultRowsMutationIndexer());
    }

    public FullTextIndexSynchronizer(Configuration configuration, SessionFactory sessionFactory,
            RowsMutationIndexer rowChangeIndexer) {
        this.synchronizationContext = new SynchronizationContext(configuration, sessionFactory);
        this.indexer = rowChangeIndexer;
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        Collection<RowsMutationReplicationEvent> events = null;
        if (event instanceof TXReplicationEvent) {
            Collection<ReplicationEvent> replicationEvents = ((TXReplicationEvent) event).getEvents();
            events = new ArrayList<RowsMutationReplicationEvent>(replicationEvents.size());
            for (ReplicationEvent replicationEvent : replicationEvents) {
                if (replicationEvent instanceof RowsMutationReplicationEvent) {
                    events.add((RowsMutationReplicationEvent) replicationEvent);
                }
            }
        } else
        if (event instanceof RowsMutationReplicationEvent) {
            events = new LinkedList<RowsMutationReplicationEvent>();
            events.add((RowsMutationReplicationEvent) event);
        }
        if (events != null && !events.isEmpty()) {
            updateIndex(events);
        }
    }

    private void updateIndex(Collection<RowsMutationReplicationEvent> events) {
        List<RowsMutation> rowsMutations = new ArrayList<RowsMutation>();
        for (RowsMutationReplicationEvent event : events) {
            String qualifiedName = event.getSchema().toLowerCase() + "." + event.getTable().toLowerCase();
            Collection<IndexingDirective> indexingDirectives =
                synchronizationContext.getIndexingDirectives(qualifiedName);
            for (IndexingDirective indexingDirective : indexingDirectives) {
                rowsMutations.add(new RowsMutation(resolveAffectedRows(event), indexingDirective));
            }
        }
        if (!rowsMutations.isEmpty()) {
            indexer.index(rowsMutations, synchronizationContext);
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
