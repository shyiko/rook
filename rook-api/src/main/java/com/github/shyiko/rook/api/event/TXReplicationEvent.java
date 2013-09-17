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
package com.github.shyiko.rook.api.event;

import java.util.List;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class TXReplicationEvent implements ReplicationEvent {

    private List<ReplicationEvent> events;

    public TXReplicationEvent(List<ReplicationEvent> events) {
        this.events = events;
    }

    public List<ReplicationEvent> getEvents() {
        return events;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TXReplicationEvent");
        sb.append("{events=[");
        for (ReplicationEvent event : events) {
            sb.append("\n    ").append(event).append(",");
        }
        if (!events.isEmpty()) {
            sb.replace(sb.length() - 1, sb.length(), "\n");
        }
        sb.append("]}");
        return sb.toString();
    }
}
