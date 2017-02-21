package org.rakam.collection;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import org.rakam.analysis.ApiKeyService;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static java.lang.String.format;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.MASTER_KEY;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;

public class EventListDeserializer
        extends JsonDeserializer<EventList>
{
    private final JsonEventDeserializer eventDeserializer;
    private final ApiKeyService apiKeyService;

    @Inject
    public EventListDeserializer(ApiKeyService apiKeyService,
            JsonEventDeserializer jsonEventDeserializer)
    {
        eventDeserializer = jsonEventDeserializer;
        this.apiKeyService = apiKeyService;
    }

    @Override
    public EventList deserialize(JsonParser jp, DeserializationContext deserializationContext)
            throws IOException
    {
        JsonToken t = jp.getCurrentToken();

        if (t != START_OBJECT) {
            throw new IllegalArgumentException("Body must be an object");
        }

        Event.EventContext context = null;

        t = jp.nextToken();

        if(t != FIELD_NAME) {
            deserializationContext.reportWrongTokenException(jp, JsonToken.FIELD_NAME, null);
        }
        String fieldName = jp.getCurrentName();
        jp.nextToken();

        TokenBuffer eventsBuffer = null;
        if (fieldName.equals("api")) {
            context = jp.readValueAs(Event.EventContext.class);
        }
        else if (fieldName.equals("events")) {
            eventsBuffer = jp.readValueAs(TokenBuffer.class);
        }
        else {
            throw new RakamException(format("Invalid property '%s'", fieldName), BAD_REQUEST);
        }

        t = jp.nextToken();
        if(t != FIELD_NAME) {
            deserializationContext.reportWrongTokenException(jp, JsonToken.FIELD_NAME, null);
        }
        fieldName = jp.getCurrentName();
        jp.nextToken();

        if (fieldName.equals("api")) {
            if (context != null) {
                throw new RakamException("multiple 'api' property", BAD_REQUEST);
            }
            context = jp.readValueAs(Event.EventContext.class);

            if (eventsBuffer == null) {
                throw new IllegalStateException();
            }

            JsonParser eventJp = eventsBuffer.asParser(jp);
            eventJp.nextToken();
            return readEvents(eventJp, context, deserializationContext);
        }
        else if (fieldName.equals("events")) {
            if (eventsBuffer != null) {
                throw new RakamException("multiple 'api' property", BAD_REQUEST);
            }

            return readEvents(jp, context, deserializationContext);
        }
        else {
            throw new RakamException(format("Invalid property '%s'", fieldName), BAD_REQUEST);
        }
    }

    private EventList readEvents(JsonParser jp, Event.EventContext context, DeserializationContext deserializationContext)
            throws IOException
    {
        List<Event> list = new ArrayList<>();

        if (jp.getCurrentToken() != JsonToken.START_ARRAY) {
            throw new RakamException("events field must be array", BAD_REQUEST);
        }

        JsonToken t = jp.nextToken();

        Object apiKey = deserializationContext.getAttribute("apiKey");
        String project = null;
        boolean masterKey = false;

        if (apiKey == null || apiKey == WRITE_KEY) {
            if(context == null) {
                throw new RakamException("api parameter is required", BAD_REQUEST);
            }
            try {
                project = apiKeyService.getProjectOfApiKey(context.apiKey,
                        apiKey == null ? WRITE_KEY : (ApiKeyService.AccessKeyType) apiKey);
            }
            catch (RakamException e) {
                masterKey = true;
            }
        }

        if (project == null) {
            masterKey = true;
            try {
                project = apiKeyService.getProjectOfApiKey(context.apiKey, MASTER_KEY);
            }
            catch (RakamException e) {
                if (e.getStatusCode() == FORBIDDEN) {
                    throw new RakamException("api_key is invalid", FORBIDDEN);
                }

                throw e;
            }
        }

        for (; t == START_OBJECT; t = jp.nextToken()) {
            list.add(eventDeserializer.deserializeWithProject(jp, project, context, masterKey));
        }

        return new EventList(context, project, list);
    }
}
