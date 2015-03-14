package org.rakam.realtime;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableList;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.model.Event;
import org.rakam.plugin.EventMapper;

import java.time.Instant;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/03/15 11:27.
 */
public class TimestampEventMapper implements EventMapper {
    @Override
    public void map(Event event) {
        if(event.properties().get("time") == null) {
            event.properties().put("time", Instant.now().getEpochSecond());
        }
    }

    @Override
    public List<SchemaField> fields() {
        return ImmutableList.of(new SchemaField("time", FieldType.LONG, false));
    }

    @Override
    public void addedFields(List<SchemaField> existingFields, List<SchemaField> newFields) {
    }
}
