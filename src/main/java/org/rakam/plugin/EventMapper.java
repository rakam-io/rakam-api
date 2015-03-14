package org.rakam.plugin;

import org.rakam.collection.SchemaField;
import org.rakam.model.Event;

import java.util.List;

/**
 * Created by buremba on 26/05/14.
 */
public interface EventMapper {
    void map(Event event);
    List<SchemaField> fields();
    void addedFields(List<SchemaField> existingFields, List<SchemaField> newFields);
}