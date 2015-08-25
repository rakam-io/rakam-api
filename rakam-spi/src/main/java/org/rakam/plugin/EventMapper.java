package org.rakam.plugin;

import org.rakam.collection.Event;
import org.rakam.collection.event.FieldDependencyBuilder;

/**
 * Created by buremba on 26/05/14.
 */
public interface EventMapper {
    void map(Event event);
    void addFieldDependency(FieldDependencyBuilder builder);
}