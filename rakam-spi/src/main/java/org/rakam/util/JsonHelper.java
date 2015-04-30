package org.rakam.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by buremba on 17/01/14.
 */
public class JsonHelper {
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static ObjectMapper prettyMapper = new ObjectMapper();

    static {
        prettyMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        SimpleModule simpleModule = new SimpleModule();
//        simpleModule.addSerializer(new SchemaSerializer());
        mapper.registerModule(simpleModule);
        mapper.registerModule(new JSR310Module());
    }

    private JsonHelper() {
    }

    private static final ObjectWriter jsonWriter = mapper.writer();
    private static final JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(false);

    public static String encode(Object obj, boolean prettyPrint) {
        try {
            return (prettyPrint ? prettyMapper : mapper).writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Object is not json serializable", e);
        }
    }

    public static String encodeSafe(Object obj) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }

    public static String encodeSafe(Object obj, boolean prettyPrint) throws JsonProcessingException {
        return (prettyPrint ? prettyMapper: mapper).writeValueAsString(obj);
    }

    public static String encode(Object obj) {
        return encode(obj, false);
    }

    public static byte[] encodeAsBytes(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static ObjectNode generate(Map<String, List<String>> map) {
        ObjectNode obj = jsonObject();
        for (Map.Entry<String, List<String>> item : map.entrySet()) {
            String key = item.getKey();
            obj.put(key, item.getValue().get(0));
        }
        return obj;
    }

    public static ObjectNode jsonObject() {
        return jsonNodeFactory.objectNode();
    }

    public static ArrayNode jsonArray() {
        return jsonNodeFactory.arrayNode();
    }

    public static <T extends JsonNode> T readSafe(String json) throws IOException {
        return (T) mapper.readTree(json);
    }

    public static <T extends JsonNode> T read(String json){
        try {
            return (T) mapper.readTree(json);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T readSafe(String json, Class<T> clazz) throws IOException {
        return mapper.readValue(json, clazz);
    }

    public static <T> T read(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T convert(Object json, Class<T> clazz) {
        try {
            return mapper.convertValue(json, clazz);
        } catch (IllegalArgumentException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T convert(Object json, TypeReference<T> ref) {
        try {
            return mapper.convertValue(json, ref);
        } catch (IllegalArgumentException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T read(byte[] json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String getOrDefault(JsonNode json, String fieldKey, String defaultValue) {
        JsonNode node = json.get(fieldKey);
        if(node != null)
            return node.asText();
        else
            return defaultValue;
    }

    public static boolean getOrDefault(JsonNode json, String fieldKey, boolean defaultValue) {
        JsonNode node = json.get(fieldKey);
        if(node != null)
            return node.asBoolean();
        else
            return defaultValue;
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }
}
