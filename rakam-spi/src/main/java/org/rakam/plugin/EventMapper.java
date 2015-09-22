package org.rakam.plugin;

import org.rakam.collection.Event;
import org.rakam.collection.event.FieldDependencyBuilder;

import java.net.InetAddress;
import java.util.Map;

/**
 * Created by buremba on 26/05/14.
 */
public interface EventMapper {
    void map(Event event, Iterable<Map.Entry<String, String>> extraProperties, InetAddress sourceAddress);
    void addFieldDependency(FieldDependencyBuilder builder);
}