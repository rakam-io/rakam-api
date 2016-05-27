package org.rakam.collection;

import com.fasterxml.jackson.core.JsonParseException;
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

import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
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

        if (t != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("body must be an object");
        }

        Event.EventContext context = null;
        String project = null;

        jp.nextToken();
        String fieldName = jp.getCurrentName();
        jp.nextToken();

        if (fieldName.equals("api")) {
            context = jp.readValueAs(Event.EventContext.class);
        }

        t = jp.nextToken();

        if (t != FIELD_NAME) {
            throw new JsonParseException("", jp.getCurrentLocation());
        }

        if (!"events".equals(jp.getText())) {
            jp.nextToken();
            fieldName = jp.getCurrentName();
            jp.nextToken();

            if (fieldName.equals("api")) {
                context = jp.readValueAs(Event.EventContext.class);
            }
        }

        if (context == null) {
            throw new RakamException("First two fields must be api and project", BAD_REQUEST);
        }

        if (t != FIELD_NAME || !"events".equals(jp.getText())) {
            throw new RakamException("The last field must be 'events'.", BAD_REQUEST);
        }

        List<Event> list = new ArrayList<>();

        if (jp.nextToken() == JsonToken.START_ARRAY) {
            t = jp.nextToken();
        } else {
            throw new RakamException("events field must be array", BAD_REQUEST);
        }

        if (project == null) {
            Object apiKey = deserializationContext.getAttribute("apiKey");
            project = apiKeyService.getProjectOfApiKey(context.writeKey, apiKey == null ? WRITE_KEY : (ApiKeyService.AccessKeyType) apiKey);
        }

        for (; t == JsonToken.START_OBJECT; t = jp.nextToken()) {
            list.add(eventDeserializer.deserializeWithProject(jp, project));
        }

        return new EventList(context, project, list);
    }
}
