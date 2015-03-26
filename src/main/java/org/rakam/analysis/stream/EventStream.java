package org.rakam.analysis.stream;

import org.rakam.plugin.user.FilterClause;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:06.
 */
public interface EventStream {
    public Supplier<CompletableFuture<Stream<Map<String, Object>>>> subscribe(String project, Map<String, FilterClause> collections, List<String> columns);
}
