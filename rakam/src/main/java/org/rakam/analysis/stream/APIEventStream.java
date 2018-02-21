package org.rakam.analysis.stream;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.google.common.collect.ImmutableList;
import org.apache.avro.generic.GenericRecord;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.stream.APIEventStreamModule.CollectionStreamHolder;
import org.rakam.analysis.stream.APIEventStreamModule.CollectionStreamHolder.CollectionFilter;
import org.rakam.collection.Event;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.stream.CollectionStreamQuery;
import org.rakam.plugin.stream.EventStream;
import org.rakam.plugin.stream.StreamResponse;
import org.rakam.presto.PrestoType;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class APIEventStream
        implements EventStream {
    private final Map<String, List<CollectionStreamHolder>> holder;
    private final ExpressionCompiler expressionCompiler;
    private final Metastore metastore;

    @Inject
    public APIEventStream(Map<String, List<CollectionStreamHolder>> holder, Metastore metastore, ExpressionCompiler expressionCompiler) {
        this.holder = holder;
        this.expressionCompiler = expressionCompiler;
        this.metastore = metastore;
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        List<CollectionFilter> collect1;
        if (collections != null) {
            collect1 = collections.stream().map(item -> {
                Predicate<GenericRecord> predicate;

                if (item.getCollection() == null) {
                    predicate = (val) -> true;
                } else {
                    List<Map.Entry<String, Type>> collect = metastore.getCollection(project, item.getCollection())
                            .stream()
                            .map((Function<SchemaField, Map.Entry<String, Type>>) f ->
                                    new SimpleImmutableEntry<>(f.getName(), PrestoType.toType(f.getType())))
                            .collect(Collectors.toList());

                    predicate = Optional.ofNullable(item.getFilter())
                            .map(value -> new SqlParser().createExpression(item.getFilter()))
                            .map(expression -> expressionCompiler.generate(expression, collect))
                            .orElse(null);
                }

                return new CollectionFilter(item.getCollection(), predicate);
            }).collect(Collectors.toList());
        } else {
            collect1 = ImmutableList.of(new CollectionFilter(null, null));
        }

        CollectionStreamHolder streamHolder = new CollectionStreamHolder(collect1);
        List<CollectionStreamHolder> holders = this.holder.computeIfAbsent(project, s -> new ArrayList<>());
        synchronized (this) {
            holders.add(streamHolder);
        }

        return new EventStreamer() {
            @Override
            public void sync() {
                Event message = streamHolder.messageQueue.poll();
                StringBuilder builder = new StringBuilder("[");

                boolean isFirst = true;
                while (message != null) {
                    if (!isFirst) {
                        builder.append(",");
                    }

                    builder.append("{\"project\":")
                            .append(JsonHelper.encode(message.project()))
                            .append(", \"collection\":")
                            .append(JsonHelper.encode(message.collection()))
                            .append(", \"properties\": ")
                            .append(message.properties().toString()).append("}");

                    isFirst = false;
                    message = streamHolder.messageQueue.poll();
                }

                builder.append("]");

                response.send("data", builder.toString());
            }

            @Override
            public void shutdown() {
                synchronized (this) {
                    holder.remove(holder);
                }
            }
        };
    }
}
