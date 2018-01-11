package org.rakam.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Throwables;
import org.rakam.server.http.SwaggerJacksonAnnotationIntrospector;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class JsonHelper {
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static ObjectMapper prettyMapper = new ObjectMapper();

    private static final JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(false);

    static {
        prettyMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new Jdk8Module());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);

        SwaggerJacksonAnnotationIntrospector ai = new SwaggerJacksonAnnotationIntrospector();

        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX"));

        mapper.registerModule(
                new SimpleModule("swagger", Version.unknownVersion()) {
                    @Override
                    public void setupModule(SetupContext context) {
                        context.insertAnnotationIntrospector(ai);
                    }
                });
    }

    private JsonHelper() {
    }

    public static String encode(Object obj, boolean prettyPrint) {
        try {
            return (prettyPrint ? prettyMapper : mapper).writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Object is not json serializable", e);
        }
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

    public static TextNode textNode(String value) {
        return jsonNodeFactory.textNode(value);
    }

    public static BinaryNode binaryNode(byte[] value) {
        return jsonNodeFactory.binaryNode(value);
    }

    public static BooleanNode booleanNode(boolean value) {
        return jsonNodeFactory.booleanNode(value);
    }

    public static NumericNode numberNode(Number value) {
        return jsonNodeFactory.numberNode((value instanceof Double || value instanceof Float) ?
                value.doubleValue() : value.longValue());
    }

    public static <T extends JsonNode> T readSafe(String json) throws IOException {
        return (T) mapper.readTree(json);
    }

    public static <T extends JsonNode> T readSafe(byte[] json) throws IOException {
        return (T) mapper.readTree(json);
    }

    public static <T extends JsonNode> T read(String json) {
        try {
            return (T) mapper.readTree(json);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T read(byte[] json, TypeReference<T> typeReference) {
        try {
            return (T) mapper.readValue(json, typeReference);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T read(String json, TypeReference<T> typeReference) {
        try {
            return (T) mapper.readValue(json, typeReference);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T extends JsonNode> T read(byte[] json) {
        try {
            return (T) mapper.readTree(json);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T readSafe(String json, Class<T> clazz) throws IOException {
        return mapper.readValue(json, clazz);
    }

    public static <T> T readSafe(byte[] json, Class<T> clazz) throws IOException {
        return mapper.readValue(json, clazz);
    }

    public static <T> T readSafe(InputStream json, Class<T> clazz) throws IOException {
        return mapper.readValue(json, clazz);
    }

    public static <T> T read(InputStream json, Class<T> clazz) throws IOException {
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

    public static ObjectMapper getMapper() {
        return mapper;
    }
}
