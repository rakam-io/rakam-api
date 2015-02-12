package org.rakam.plugin;

import org.apache.avro.Schema;
import org.rakam.model.Event;

import java.util.List;

/**
 * Created by buremba on 26/05/14.
 */
public interface EventMapper {
    void map(Event event);
    List<Schema.Field> fields();
}