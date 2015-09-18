/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.presto.stream.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 18/09/15 06:43.
 */
public class EventStreamProcessor implements Callable<Boolean>, HazelcastInstanceAware {
    private final List<byte[]> events;
    private final String id;
    private HazelcastInstance hazelcastInstance;

    public EventStreamProcessor(String id, List<byte[]> events) {
        this.id = id;
        this.events = events;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public Boolean call() {
        Map<String, Queue<List<byte[]>>> eventStreamHandlers = (Map) hazelcastInstance.getUserContext()
                .get("eventStream");
        Queue<List<byte[]>> lists = eventStreamHandlers.get(id);
        if (lists != null) {
            lists.add(events);
            return true;
        } else {
            return false;
        }
    }
}
