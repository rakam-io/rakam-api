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
package org.rakam.presto.stream.streamer;

import com.google.common.collect.Lists;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/09/15 04:34.
 */
public class FetchEventStreamExecutor implements Callable, HazelcastInstanceAware {
    private String ticket;
    private transient HazelcastInstance instance;

    public FetchEventStreamExecutor(String ticket) {
        this.ticket = ticket;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
    }

    @Override
    public List<String> call() throws Exception {
        Map<String, Queue<String>> eventListener = (Map<String, Queue<String>>) instance
                .getUserContext().get("eventListener");
        Queue<String> elements = eventListener.get(ticket);
        List<String> strings = Lists.newArrayList(elements);
        elements.clear();
        return strings;
    }
}
