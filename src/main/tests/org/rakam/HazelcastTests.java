package org.rakam;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/07/14 05:27.
 */

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import org.rakam.cache.hazelcast.LimitPredicate;
import org.rakam.cache.hazelcast.models.SimpleCounter;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class HazelcastTests {

    public static void main(String[] args) throws Exception {

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(new Config());
        IMap<String, SimpleCounter> map = hz.getMap("map");
        map.addIndex("this", true);

        for (int k = 0; k < 50000; k++) {
            String substring = UUID.randomUUID().toString().substring(10);
            map.put(substring, new SimpleCounter(k));
        }


        Map.Entry<String, SimpleCounter>[] values = (Map.Entry<String, SimpleCounter>[]) map.entrySet(new LimitPredicate("this", 10)).toArray(new Map.Entry[0]);
        long l1 = System.currentTimeMillis();
        Arrays.sort(values, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        long l = System.currentTimeMillis();
        JobTracker tracker = hz1.getJobTracker("myJobTracker");
        KeyValueSource<String, SimpleCounter> kvs = KeyValueSource.fromMap(map);
        Job<String, SimpleCounter> job = tracker.newJob(kvs);
        JobCompletableFuture<Map<String, SimpleCounter[]>> myMapReduceFuture =
                job.mapper(new MyMapper())
                        .reducer(new MyReducerFactory())
                        .submit();

        Map<String, SimpleCounter[]> result = myMapReduceFuture.get();

        System.out.println(result.get(""));
        long l2 = System.currentTimeMillis();
        System.out.println(l2-l1);
    }

    public static class MyMapper implements Mapper<String, SimpleCounter, String, SimpleCounter> {

        @Override
        public void map(String key, SimpleCounter value, Context<String, SimpleCounter> context) {
            context.emit("", value);
        }
    }

    /**
     * Returns a Reducer. Multiple reducers run on one Node, therefore we must provide a factory.
     */
    public static class MyReducerFactory implements com.hazelcast.mapreduce.ReducerFactory<String, SimpleCounter, SimpleCounter[]> {

        @Override
        public Reducer<String, SimpleCounter, SimpleCounter[]> newReducer(String key) {
            return new MyReducer();
        }
    }

    public static class MyReducer extends Reducer<String, SimpleCounter, SimpleCounter[]> {

        private SimpleCounter[] sum = new SimpleCounter[10];

        @Override
        public void reduce(SimpleCounter value) {
            for(int i=0; i<10; i++) {

                if(sum[i]==null) {
                    sum[i] = value;
                    break;
                }else
                if(value.getValue()>sum[i].getValue()) {

                    for (int a = (10 - 2); a >= i; a--) {
                        sum[a+1] = sum[a];
                    }
                    sum[i] = value;
                    break;
                }
            }
        }

        @Override
        public SimpleCounter[] finalizeReduce() {
            return sum;
        }
    }

}
