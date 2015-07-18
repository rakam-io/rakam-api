package org.rakam.analysis.stream;

import com.facebook.presto.spi.FixedPageSource;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.AbstractEntryProcessor;
import org.rakam.analysis.stream.processor.Processor;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
* Created by buremba <Burak Emre Kabakcı> on 13/07/15 14:40.
*/
class StreamQueryExecutor extends AbstractEntryProcessor<String, Processor> implements HazelcastInstanceAware {
    private final String sql;
    private QueryAnalyzer queryExecutor;

    public StreamQueryExecutor(String sql) {
        this.sql = sql;
    }

    @Override
    public Object process(Map.Entry<String, Processor> entry) {
        MaterializedOutputFactory outputFactory = new MaterializedOutputFactory(queryExecutor.getSession());

        List<SchemaField> schema = entry.getValue().getSchema();
        queryExecutor.execute(sql,
                (split, columns) ->
                        new FixedPageSource(() -> entry.getValue().getOutput()), schema, outputFactory).execute();

        MaterializingOperator materializingOperator = outputFactory.getMaterializingOperator();
        List<List<Object>> materializedResult = materializingOperator.getMaterializedResult();
        List<SchemaField> collect = materializingOperator.getTypes().stream()
                .map(type -> new SchemaField("", RakamMetadata.convertColumn(type), false))
                .collect(Collectors.toList());
        return new QueryResult(collect, materializedResult);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.queryExecutor = (QueryAnalyzer) hazelcastInstance.getUserContext()
                .computeIfAbsent("queryExecutor", (key) -> new QueryAnalyzer());
    }
}
