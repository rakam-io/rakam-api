package org.rakam.analysis.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by buremba on 16/01/14.
 */
public abstract class AggregationRule implements DataSerializable {
    private String id;
    public String project;
    public String groupBy;
    public HashMap<String, String> filters;
    public AggregationType type;
    public String select;

    public AggregationRule() {}

    public AggregationRule(String projectId, AggregationType type)   {
        this(projectId, type, null, null, null);
    }

    @Override
    public boolean equals(Object object)
    {
        return object != null && object instanceof AggregationRule && ((AggregationRule) object).id.equals(id);
    }

    public String id() {
        return id;
    }

    public AggregationRule(String project, AggregationType type, String select) {
        this(project, type, select, null, null);
    }

    public AggregationRule(String project, AggregationType type, String select, HashMap<String, String> filters) {
        this(project, type, select, filters, null);
    }

    public AggregationRule(String project, AggregationType type, String select, HashMap<String, String> filters, String groupBy) {
        this.id = buildId(project, type, select, filters, groupBy);
        this.groupBy = groupBy;
        this.type = type;
        this.select = select;
        this.filters = filters;
        this.project = project;
    }

    public void readData(ObjectDataInput in) throws IOException {
        id = in.readUTF();
        filters  = in.readObject();
        groupBy = in.readUTF();
        select = in.readUTF();
        type = AggregationType.get(in.readShort());
        project = in.readUTF();
    }

    public abstract Analysis analysisType();

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeObject(filters);
        out.writeUTF(groupBy);
        out.writeUTF(select);
        out.writeShort(type.id);
        out.writeUTF(project);
    }

    public int hashCode() {
        return Arrays.hashCode(id().getBytes());
    }

    public abstract String buildId(String project, AggregationType agg_type, String select, HashMap<String,String> filters, String groupBy);
}
