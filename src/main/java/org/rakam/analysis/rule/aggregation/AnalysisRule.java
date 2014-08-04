package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 05/05/14.
 */
public abstract class AnalysisRule implements IdentifiedDataSerializable {
    public String project;
    public String id;
    public AnalysisRuleStrategy strategy = AnalysisRuleStrategy.REAL_TIME;
    public boolean batch_status = false;

    public abstract Analysis analysisType();
    public abstract int hashCode();
    public abstract JsonObject toJson();

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }
}
