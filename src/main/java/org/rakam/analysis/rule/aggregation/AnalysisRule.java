package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by buremba on 05/05/14.
 */
public abstract class AnalysisRule implements DataSerializable {
    public String project;
    public String id;
    public AnalysisRuleStrategy strategy = AnalysisRuleStrategy.REAL_TIME_AFTER_BATCH;
    public boolean batch_status = false;

    public abstract Analysis analysisType();
    public abstract void readData(ObjectDataInput in) throws IOException;
    public abstract void writeData(ObjectDataOutput out) throws IOException;
    public abstract int hashCode();
    public abstract JsonObject toJson();
}
