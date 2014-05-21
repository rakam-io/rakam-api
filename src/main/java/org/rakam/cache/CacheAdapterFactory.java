package org.rakam.cache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.IMap;
import org.rakam.analysis.rule.AnalysisRuleList;

/**
 * Created by buremba on 22/12/13.
 */
public class CacheAdapterFactory {
    public static IMap<String, AnalysisRuleList> getAggregationMap() {
        ClientConfig clientConfig = new ClientConfig();
        //clientConfig.getSerializationConfig().addDataSerializableFactory(AggregationRuleListFactory.ID, new AggregationRuleListFactory());
        clientConfig.getGroupConfig().setName("analytics").setPassword("");
        return HazelcastClient.newHazelcastClient(clientConfig).getMap("aggregation.rules");
    }
}
