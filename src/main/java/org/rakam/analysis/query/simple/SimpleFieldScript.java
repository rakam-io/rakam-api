package org.rakam.analysis.query.simple;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import org.rakam.analysis.query.FieldScript;
import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.io.IOException;

/**
 * Created by buremba on 04/05/14.
 */
public class SimpleFieldScript<T> implements FieldScript<T>, JsonSerializable {
    private final String fieldKey;
    final transient String userData;

    public SimpleFieldScript(String fieldKey) {
        this.fieldKey = fieldKey;
        userData = fieldKey.startsWith("_user.") ? fieldKey.substring(6) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleFieldScript)) return false;

        SimpleFieldScript that = (SimpleFieldScript) o;

        if (!fieldKey.equals(that.fieldKey)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return fieldKey.hashCode();
    }

    @Override
    public boolean requiresUser() {
        return userData != null;
    }

    @Override
    public JsonElement toJson() {
        return JsonElement.valueOf(fieldKey);
    }

    @Override
    public T extract(JsonObject event, JsonObject user) {
        if (userData != null)
            return user == null ? null : (T) user.getValue(userData);
        return (T) event.getValue(fieldKey);
    }

    @Override
    public boolean contains(JsonObject event, JsonObject user) {
        return extract(event, user) != null;
    }

    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        jgen.writeString(fieldKey);
    }

    @Override
    public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException, JsonProcessingException {

    }
}
