package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Created by buremba on 05/05/14.
 */
public abstract class AnalysisRule implements IdentifiedDataSerializable {
    public String project;
    public AnalysisRuleStrategy strategy = AnalysisRuleStrategy.REAL_TIME;
    public boolean batch_status = false;
    private String id;

    public String id() {
        if(id==null) {
            id = Base64.getEncoder().encodeToString(ByteBuffer.allocate(4).putInt(hashCode()).array());
        }
        return id;
    }

    public abstract Analysis analysisType();
    public abstract JsonObject toJson();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnalysisRule)) return false;

        AnalysisRule that = (AnalysisRule) o;

        if (!project.equals(that.project)) return false;
        if (strategy != that.strategy) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + strategy.hashCode();
        return result;
    }

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }
}
