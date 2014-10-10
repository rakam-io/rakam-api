package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by buremba on 16/01/14.
 */
public abstract class AggregationRule extends AnalysisRule {
    public FieldScript<String> groupBy;
    public FilterScript filters;
    public AggregationType type;
    public FieldScript<String> select;

    protected AggregationRule() {
    }

    public AggregationRule(String project, AggregationType type, FieldScript select) {
        this(project, type, select, null, null);
    }

    public AggregationRule(String project, AggregationType type) {
        this(project, type, null, null, null);
    }


    public AggregationRule(String project, AggregationType type, FieldScript select, FilterScript filters) {
        this(project, type, select, filters, null);
    }

    public AggregationRule(String project, AggregationType type, FieldScript select, FilterScript filters, FieldScript groupBy) {
        this.groupBy = groupBy;
        this.type = type;
        this.select = select;
        this.filters = filters;
        this.project = project;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregationRule)) return false;
        if (!super.equals(o)) return false;

        AggregationRule that = (AggregationRule) o;

        if (filters != null ? !filters.equals(that.filters) : that.filters != null) return false;
        if (groupBy != null ? !groupBy.equals(that.groupBy) : that.groupBy != null) return false;
        if (select != null ? !select.equals(that.select) : that.select != null) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (groupBy != null ? groupBy.hashCode() : 0);
        result = 31 * result + (filters != null ? filters.hashCode() : 0);
        result = 31 * result + type.hashCode();
        result = 31 * result + (select != null ? select.hashCode() : 0);
        return result;
    }

    @Override
    public boolean canAnalyze(AggregationAnalysis analysis) {
        return Arrays.asList(analysis.getAnalyzableAggregationTypes()).contains(type);
    }


    public void readData(ObjectDataInput in) throws IOException {
        filters = in.readObject();
        groupBy = in.readObject();
        select = in.readObject();
        type = AggregationType.get(in.readShort());
        project = in.readUTF();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(filters);
        out.writeObject(groupBy);
        out.writeObject(select);
        out.writeShort(type.id);
        out.writeUTF(project);
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.putString("tracking", project);
        json.putString("strategy", strategy.name());
        json.putString("analysis", analysisType().name().replaceFirst("ANALYSIS_", ""));
        if (select != null)
            json.putValue("select", select.toJson());
        if (groupBy != null)
            json.putString("group_by", groupBy.toJson());
        if (filters != null)
            json.putString("filters", filters.toJson());
        json.putString("aggregation", type.name());
        return json;
    }
}
