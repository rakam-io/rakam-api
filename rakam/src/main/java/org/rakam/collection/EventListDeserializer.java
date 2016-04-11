package org.rakam.collection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.rakam.analysis.ApiKeyService;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;

public class EventListDeserializer extends JsonDeserializer<EventList> {
    private final JsonEventDeserializer eventDeserializer;
    private final ApiKeyService apiKeyService;

    @Inject
    public EventListDeserializer(ApiKeyService apiKeyService,
                                 JsonEventDeserializer jsonEventDeserializer) {
        eventDeserializer = jsonEventDeserializer;
        this.apiKeyService = apiKeyService;
    }

    @Override
    public EventList deserialize(JsonParser jp, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
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

        if(context == null) {
            throw new RakamException("First two fields must be api and project", BAD_REQUEST);
        }

        jp.nextToken();
        if (!"events".equals(jp.getCurrentName())) {
            throw new RakamException("Third field must be events.", BAD_REQUEST);
        }

        t = jp.nextToken();

        List<Event> list = new ArrayList<>();

        if (t == JsonToken.START_ARRAY) {
            t = jp.nextToken();
        } else {
            throw new RakamException("events field must be array", BAD_REQUEST);
        }

        if(project == null) {
            project = apiKeyService.getProjectOfApiKey(context.writeKey, WRITE_KEY);
        }

        for (; t == JsonToken.START_OBJECT; t = jp.nextToken()) {
            list.add(eventDeserializer.deserializeWithProject(jp, project));
        }

        return new EventList(context, project, list);
    }
}
