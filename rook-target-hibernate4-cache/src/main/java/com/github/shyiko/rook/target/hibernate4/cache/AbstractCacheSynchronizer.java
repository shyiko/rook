/*
 * Copyright 2015 Igor Grunskiy
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
import com.github.shyiko.rook.api.event.InsertRowsReplicationEvent;
import com.github.shyiko.rook.api.event.ReplicationEvent;
import com.github.shyiko.rook.api.event.RowsMutationReplicationEvent;
import com.github.shyiko.rook.api.event.TXReplicationEvent;
import com.github.shyiko.rook.api.event.UpdateRowsReplicationEvent;
import com.github.shyiko.rook.api.event.DeleteRowsReplicationEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:igor.grunskyi@gmail.com">Igor Grunskiy</a>
 */
public abstract class AbstractCacheSynchronizer implements ReplicationEventListener {

    protected final SynchronizationContext synchronizationContext;

    protected AbstractCacheSynchronizer(SynchronizationContext synchronizationContext) {
        this.synchronizationContext = synchronizationContext;
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
        } else if (event instanceof RowsMutationReplicationEvent) {
            events = new LinkedList<RowsMutationReplicationEvent>();
            events.add((RowsMutationReplicationEvent) event);
        }
        if (events != null && !events.isEmpty()) {
            processTX(events);
        }
    }

    protected List<Serializable[]> resolveAffectedRows(RowsMutationReplicationEvent event) {
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

    protected abstract void processTX(Collection<RowsMutationReplicationEvent> txEvents);

}
