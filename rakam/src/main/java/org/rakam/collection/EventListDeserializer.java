package org.rakam.collection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EventListDeserializer extends JsonDeserializer<EventCollectionHttpService.EventList> {
    private final JsonEventDeserializer2 eventDeserializer;

    @Inject
    public EventListDeserializer(Metastore metastore, FieldDependencyBuilder.FieldDependency fieldDependency) {
        eventDeserializer = new JsonEventDeserializer2(metastore, fieldDependency);
    }

    @Override
    public EventCollectionHttpService.EventList deserialize(JsonParser jp, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonToken t = jp.getCurrentToken();

        if(t != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("body must be an object");
        }

        Event.EventContext context = null;
        String project = null;

        jp.nextToken();
        String fieldName = jp.getCurrentName();
        jp.nextToken();

        if(fieldName.equals("api")) {
            context = jp.readValueAs(Event.EventContext.class);
        } else
        if(fieldName.equals("project")) {
            project = jp.getValueAsString();
        }


        jp.nextToken();
        fieldName = jp.getCurrentName();
        jp.nextToken();

        if(fieldName.equals("api")) {
            context = jp.readValueAs(Event.EventContext.class);
        } else
        if(fieldName.equals("project")) {
            project = jp.getValueAsString();
        }

        if(project == null || context == null) {
            throw new RakamException("First two fields must be api and project", HttpResponseStatus.BAD_REQUEST);
        }

        jp.nextToken();
        if (!"events".equals(jp.getCurrentName())) {
            throw new RakamException("Third field must be events.", HttpResponseStatus.BAD_REQUEST);
        }

        t = jp.nextToken();

        List<Event> list = new ArrayList<>();

        if (t == JsonToken.START_ARRAY) {
            t = jp.nextToken();
        } else {
            throw new RakamException("events field must be array", HttpResponseStatus.BAD_REQUEST);
        }

        for (; t == JsonToken.START_OBJECT; t = jp.nextToken()) {
            list.add(eventDeserializer.deserializeWithProject(jp, project));
        }

        return new EventCollectionHttpService.EventList(context, project, list);
    }
}
