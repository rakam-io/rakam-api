package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.util.SpanTime;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by buremba on 16/01/14.
 */
public class TimeSeriesAggregationRule extends AggregationRule {
    public static final Analysis TYPE = Analysis.ANALYSIS_TIMESERIES;
    public SpanTime interval;

    public TimeSeriesAggregationRule(String projectId, AggregationType type, SpanTime interval, FieldScript select) {
        super(projectId, type, select);
        this.interval = interval;
    }

    public TimeSeriesAggregationRule(String projectId, AggregationType type, SpanTime interval, FieldScript select, FilterScript filters) {
        super(projectId, type, select, filters);
        this.interval = interval;
    }

    public TimeSeriesAggregationRule(String projectId, AggregationType type, SpanTime interval, FieldScript select, FilterScript filters, FieldScript groupBy) {
        super(projectId, type, select, filters, groupBy);
        this.interval = interval;
        this.id = buildId();
    }

    @Override
    public Analysis analysisType() {
        return TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        interval.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        interval = new SpanTime(in.readInt());
    }

    public String buildId() {
        return project+type+select+groupBy+filters+interval;
    }

    public static String buildId(String project, AggregationType agg_type, FieldScript select, FilterScript filters, FieldScript groupBy, SpanTime interval) {
        return project+agg_type+select+groupBy+filters+interval;
    }

    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.putString("interval", interval.toString());
        return json;
    }
}
