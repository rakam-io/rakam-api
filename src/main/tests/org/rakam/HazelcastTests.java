package org.rakam;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/07/14 05:27.
 */

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.rakam.stream.SimpleCounter;

public class HazelcastTests {

    public static void main(String[] args) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(new Config());

        long l = System.currentTimeMillis();
        IMap<String, Integer> test = hz.getMap("test");
        for (int i = 0; i < 500000; i++) {
            test.put("test"+i, 3);
        }
        System.out.println(System.currentTimeMillis() - l);

    }

    public static void mains(String[] args) throws Exception {

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig("config/cluster.xml"));
        for (int i = 0; i < 10; i++) {
            hz.getAtomicLong(".."+i+"..").incrementAndGet();
        }
//        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(new Config());
//        IMap<String, DefaultCounter> map = hz.getMap("map");
//        map.addIndex("this", true);
//
//        for (int k = 0; k < 50000; k++) {
//            String substring = UUID.randomUUID().toString().substring(10);
//            map.put(substring, new DefaultCounter(k));
//        }
//
//
//        Map.Entry<String, DefaultCounter>[] values = (Map.Entry<String, DefaultCounter>[]) map.entrySet(new LimitPredicate("this", 10)).toArray(new Map.Entry[0]);
//        long l1 = System.currentTimeMillis();
//        Arrays.sort(values, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
//
//        long l = System.currentTimeMillis();
//        JobTracker tracker = hz1.getJobTracker("myJobTracker");
//        KeyValueSource<String, DefaultCounter> kvs = KeyValueSource.fromMap(map);
//        Job<String, DefaultCounter> job = tracker.newJob(kvs);
//        JobCompletableFuture<Map<String, DefaultCounter[]>> myMapReduceFuture =
//                job.mapper(new MyMapper())
//                        .reducer(new MyReducerFactory())
//                        .submit();
//
//        Map<String, DefaultCounter[]> result = myMapReduceFuture.get();
//
//        System.out.println(result.get(""));
//        long l2 = System.currentTimeMillis();
//        System.out.println(l2-l1);
    }

    public static class MyMapper implements Mapper<String, SimpleCounter, String, SimpleCounter> {

        @Override
        public void map(String key, SimpleCounter value, Context<String, SimpleCounter> context) {
            context.emit("", value);
        }
    }
}
