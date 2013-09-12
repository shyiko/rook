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
package com.github.shyiko.rook.it.h4com;

import com.github.shyiko.rook.api.ReplicationEventListener;
import com.github.shyiko.rook.api.event.ReplicationEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class CountDownReplicationListener implements ReplicationEventListener {

    private final Map<Class<? extends ReplicationEvent>, AtomicInteger> countersByDataClass =
        new HashMap<Class<? extends ReplicationEvent>, AtomicInteger>();

    @Override
    public void onEvent(ReplicationEvent event) {
        incrementCounter(getCounter(event.getClass()));
    }

    private AtomicInteger getCounter(Class<? extends ReplicationEvent> key) {
        synchronized (countersByDataClass) {
            AtomicInteger counter = countersByDataClass.get(key);
            if (counter == null) {
                countersByDataClass.put(key, counter = new AtomicInteger());
            }
            return counter;
        }
    }

    private void incrementCounter(AtomicInteger counter) {
        synchronized (counter) {
            if (counter.incrementAndGet() == 0) {
                counter.notify();
            }
        }
    }

    public void waitFor(Class<? extends ReplicationEvent> dataClass, int numberOfEvents, long timeoutInMilliseconds)
            throws TimeoutException, InterruptedException {
        waitForCounterToGetZero(dataClass.getSimpleName(), getCounter(dataClass), numberOfEvents,
            timeoutInMilliseconds);
    }

    private void waitForCounterToGetZero(String counterName, AtomicInteger counter, int numberOfExpectedEvents,
            long timeoutInMilliseconds) throws TimeoutException, InterruptedException {
        synchronized (counter) {
            counter.set(counter.get() - numberOfExpectedEvents);
            if (counter.get() != 0) {
                counter.wait(timeoutInMilliseconds);
                if (counter.get() != 0) {
                    throw new TimeoutException("Received " + (numberOfExpectedEvents + counter.get()) + " " +
                        counterName + " event(s) instead of expected " + numberOfExpectedEvents);
                }
            }
        }
    }

    public void reset() {
        synchronized (countersByDataClass) {
            countersByDataClass.clear();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CountDownEventListener{");
        sb.append("countersByDataClass=").append(countersByDataClass);
        sb.append('}');
        return sb.toString();
    }

}
