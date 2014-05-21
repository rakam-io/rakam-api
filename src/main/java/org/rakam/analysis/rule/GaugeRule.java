package org.rakam.analysis.rule;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.constant.Analysis;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by buremba on 05/05/14.
 */
public class GaugeRule extends AnalysisRule {
    @Override
    public Analysis analysisType() {
        return Analysis.ANALYSIS_GAUGE;
    }

    public GaugeRule(String project) {
        this.project = project;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public JsonObject toJson() {
        return null;
    }
}
