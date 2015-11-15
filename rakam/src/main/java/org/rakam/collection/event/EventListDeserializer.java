package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.Event;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EventListDeserializer extends JsonDeserializer<EventCollectionHttpService.EventList> {
    private final EventDeserializer eventDeserializer;

    @Inject
    public EventListDeserializer(Metastore metastore, FieldDependencyBuilder.FieldDependency fieldDependency) {
        eventDeserializer = new EventDeserializer(metastore, fieldDependency);
    }

    @Override
    public EventCollectionHttpService.EventList deserialize(JsonParser jp, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonToken t = jp.getCurrentToken();

        if(t != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("Must be an object");
        }

        jp.nextToken();

        String fieldName = jp.getCurrentName();
        if(!fieldName.equals("api")) {
            throw new RakamException("The first field must be api", HttpResponseStatus.BAD_REQUEST);
        }

        jp.nextToken();
        Event.EventContext context = jp.readValueAs(Event.EventContext.class);
        jp.nextToken();

        fieldName = jp.getCurrentName();
        if(!fieldName.equals("project")) {
            throw new RakamException("The second field must be project", HttpResponseStatus.BAD_GATEWAY);
        }

        jp.nextToken();
        String project = jp.getValueAsString();
        jp.nextToken();

        fieldName = jp.getCurrentName();
        if(!fieldName.equals("events")) {
            throw new RakamException("The second field must be project", HttpResponseStatus.BAD_REQUEST);
        }

        t = jp.nextToken();

        List<Event> list = new ArrayList<>();

        if (t == JsonToken.START_ARRAY) {
            t = jp.nextToken();
        } else {
            throw new RakamException("events field must be array", HttpResponseStatus.BAD_REQUEST);
        }

        for (; t != JsonToken.END_ARRAY; t = jp.nextToken()) {
            list.add(eventDeserializer.deserializeWithProject(jp, project));
        }

        return new EventCollectionHttpService.EventList(project, list, context);
    }
}
