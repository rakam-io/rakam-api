package org.rakam.plugin;

import org.rakam.collection.Event;
import org.rakam.collection.event.FieldDependencyBuilder;

import java.net.InetAddress;
import java.util.Map;


public interface EventMapper {
    void map(Event event, Iterable<Map.Entry<String, String>> extraProperties, InetAddress sourceAddress);
    void addFieldDependency(FieldDependencyBuilder builder);
}