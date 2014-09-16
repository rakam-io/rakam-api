package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by buremba on 16/01/14.
 */
public abstract class AggregationRule extends AnalysisRule {
    public FieldScript<String> groupBy;
    public FilterScript filters;
    public AggregationType type;
    public FieldScript<String> select;

    public AggregationRule(String projectId, AggregationType type)   {
        this(projectId, type, null, null, null);
    }

    protected AggregationRule() {}

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

    public AggregationRule(String project, AggregationType type, FieldScript select) {
        this(project, type, select, null, null);
    }

    public boolean canAnalyze(AggregationAnalysis analysis) {
       return analysis.getAggregationType().equals(this.type);
    }

    public AggregationRule(String project, AggregationType type, FieldScript select, FilterScript filters) {
        this(project, type, select, filters, null);
    }

    public AggregationRule(String project, AggregationType type, FieldScript select, FilterScript filters, FieldScript groupBy) {
        super();
        this.groupBy = groupBy;
        this.type = type;
        this.select = select;
        this.filters = filters;
        this.project = project;
    }

    public void readData(ObjectDataInput in) throws IOException {
        filters  = in.readObject();
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
        json.putString("id", id());
        json.putString("_tracking", project);
        json.putString("strategy", strategy.name());
        json.putString("analysis", analysisType().name().replaceFirst("ANALYSIS_", ""));
        if(select!=null)
            json.putString("select", select.toString());
        if(groupBy!=null)
            json.putString("group_by", groupBy.toString());
        if(filters!=null)
            json.putString("filters", filters.toString());
        json.putString("aggregation", type.name());
        return json;
    }
}
