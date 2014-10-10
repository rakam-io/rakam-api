package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.util.Objects;

/**
 * Created by buremba on 16/01/14.
 */
public class TimeSeriesAggregationRule extends AggregationRule {
    public static final Analysis TYPE = Analysis.ANALYSIS_TIMESERIES;
    public int interval;

    public TimeSeriesAggregationRule(String projectId, AggregationType type, int interval) {
        super(projectId, type);
        this.interval = interval;
    }

    public TimeSeriesAggregationRule(String projectId, AggregationType type, int interval, FieldScript select) {
        super(projectId, type, select);
        this.interval = interval;
    }

    public  TimeSeriesAggregationRule(String projectId, AggregationType type, int interval, FieldScript select, FilterScript filters) {
        super(projectId, type, select, filters);
        this.interval = interval;
    }

    public  TimeSeriesAggregationRule(String projectId, AggregationType type, int interval, FieldScript select, FilterScript filters, FieldScript groupBy) {
        super(projectId, type, select, filters, groupBy);
        this.interval = interval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimeSeriesAggregationRule)) return false;
        if (!super.equals(o)) return false;

        TimeSeriesAggregationRule that = (TimeSeriesAggregationRule) o;

        if (interval!=that.interval) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + interval;
        return result;
    }

    @Override
    public Analysis analysisType() {
        return TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(interval);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        interval = in.readInt();
    }

    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.putNumber("interval", interval);
        return json;
    }

    public boolean isMultipleInterval(TimeSeriesAggregationRule rule) {
        return rule.project.equals(project) &&
                rule.type.equals(type) && Objects.equals(rule.select, select) &&
                Objects.equals(rule.filters, filters) && Objects.equals(rule.groupBy, groupBy)
                && interval % rule.interval == 0;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.TIMESERIES_AGGREGATION_RULE;
    }
}
