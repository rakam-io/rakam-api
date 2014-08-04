package org.rakam;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/07/14 05:14.
 */
public class SumTask implements Callable<Integer>, Serializable, HazelcastInstanceAware {
    private transient HazelcastInstance hz;

    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }

    public Integer call() throws Exception {
        IMap<String, Integer> map = hz.getMap("map");

        System.out.println(hz.getLocalEndpoint());
        int result = 0;
        for (String key : map.localKeySet()) {
            System.out.println("Calculating for key: " + key);
            result += map.get(key);
        }
        System.out.println("Local Result: " + result);
        return result;
    }
}
