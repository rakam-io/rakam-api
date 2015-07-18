package org.rakam.analysis.stream;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.rakam.analysis.worker.IEmitter;
import org.rakam.collection.Event;
import org.rakam.plugin.ContinuousQuery;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 07/07/15 03:17.
 */
public class StreamingEmitter implements IEmitter<Event> {
    private final RakamContinuousQueryService service;

    public StreamingEmitter(RakamContinuousQueryService service) {
        this.service = service;
    }

    @Override
    public List<Event> emit(List<Event> buffer) throws IOException {
        buffer.stream().collect(Collectors.groupingBy(x -> x.project()))
                .forEach((project, events) -> {
                    List<ContinuousQuery> list = service.list(project);
                    for (ContinuousQuery continuousQuery : list) {
                        HashSet<String> collections = new HashSet<>(continuousQuery.collections);

                        service.updateReport(continuousQuery, Iterables.filter(events, new Predicate<Event>() {
                            @Override
                            public boolean apply(Event input) {
                                return collections.contains(input.collection());
                            }
                        }));
                    }
                });

        return null;
    }

    @Override
    public void fail(List<Event> records) {

    }

    @Override
    public void shutdown() {

    }
}
