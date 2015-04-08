package org.rakam.realtime;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableList;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;

import java.time.Instant;

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
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields(ImmutableList.of(new SchemaField("time", FieldType.LONG, false)));
    }


}
