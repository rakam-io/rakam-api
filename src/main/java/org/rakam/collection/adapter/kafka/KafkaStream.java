package org.rakam.collection.adapter.kafka;

import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.google.inject.Inject;
import org.rakam.analysis.stream.EventStream;
import org.rakam.model.Event;
import org.rakam.plugin.user.FilterCriteria;
import org.rakam.report.PrestoQueryExecutor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:13.
 */
public class KafkaStream implements EventStream {

    private final KafkaOffsetManager offsetManager;
    private final PrestoQueryExecutor prestoExecutor;

    @Inject
    public KafkaStream(KafkaOffsetManager offsetManager, PrestoQueryExecutor prestoExecutor) {
        this.offsetManager = offsetManager;
        this.prestoExecutor = prestoExecutor;
    }

    @Override
    public Supplier<List<Event>> subscribe(String project, Set<String> collections, List<FilterCriteria> filters) {
        return new KafkaEventSupplier(project, collections, filters);
    }

    public class KafkaEventSupplier implements Supplier<List<Event>> {
        private final List<FilterCriteria> filters;
        Map<String, Long> offsets;

        public KafkaEventSupplier(String project, Set<String> collections, List<FilterCriteria> filters) {
            this.offsets = offsetManager.getOffset(project, collections);
            this.filters = filters;
        }

        @Override
        public List<Event> get() {
            return Lists.newArrayList();
        }
    }
}
