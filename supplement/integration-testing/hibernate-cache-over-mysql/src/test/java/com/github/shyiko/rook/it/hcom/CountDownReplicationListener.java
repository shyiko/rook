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
package com.github.shyiko.rook.it.hcom;

import com.github.shyiko.rook.api.ReplicationListener;
import com.github.shyiko.rook.api.event.ReplicationEvent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * todo(shyiko): new CountDownReplicationListener().waitFor(eventClass, numberOfEvents, timeout) would be better
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class CountDownReplicationListener implements ReplicationListener {

    private final Class<? extends ReplicationEvent> eventClass;
    private CountDownLatch latch;

    public CountDownReplicationListener(Class<? extends ReplicationEvent> eventClass, int numberOfEvents) {
        this.eventClass = eventClass;
        this.latch = new CountDownLatch(numberOfEvents);
    }

    @Override
    public void onEvent(ReplicationEvent event) {
        if (eventClass.isInstance(event)) {
            latch.countDown();
        }
    }

    public boolean waitForCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }
}
