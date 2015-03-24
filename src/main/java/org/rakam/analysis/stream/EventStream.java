package org.rakam.analysis.stream;

import org.rakam.model.Event;
import org.rakam.plugin.user.FilterCriteria;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:06.
 */
public interface EventStream {
    public Supplier<List<Event>> subscribe(String project, Set<String> collections, List<FilterCriteria> filters);
}
