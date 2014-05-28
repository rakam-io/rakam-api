package org.rakam.analysis.rule.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.constant.AggregationType;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by buremba on 16/01/14.
 */
public abstract class AggregationRule extends AnalysisRule {
    public FieldScript groupBy;
    public FilterScript filters;
    public AggregationType type;
    public FieldScript select;

    public AggregationRule(String projectId, AggregationType type)   {
        this(projectId, type, null, null, null);
    }

    @Override
    public boolean equals(Object object)
    {
        return object != null && object instanceof AggregationRule && ((AggregationRule) object).id.equals(id);
    }

    public AggregationRule(String project, AggregationType type, FieldScript select) {
        this(project, type, select, null, null);
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

    public void readData(ObjectDataInput in) throws IOException {
        id = in.readUTF();
        filters  = in.readObject();
        groupBy = in.readObject();
        select = in.readObject();
        type = AggregationType.get(in.readShort());
        project = in.readUTF();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeObject(filters);
        out.writeObject(groupBy);
        out.writeObject(select);
        out.writeShort(type.id);
        out.writeUTF(project);
    }

    public int hashCode() {
        return Arrays.hashCode(id.getBytes());
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.putString("id", id);
        json.putString("project", project);
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

    public abstract String buildId();
}
