package org.rakam.util.json.impl;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 12:53.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.rakam.util.json.DecodeException;
import org.rakam.util.json.EncodeException;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Json {

    private final static ObjectMapper mapper = new ObjectMapper();
    private final static ObjectMapper prettyMapper = new ObjectMapper();

    static {
        prettyMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public static String encode(Object obj) throws EncodeException {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new EncodeException("Failed to encode as JSON: " + e.getMessage());
        }
    }

    public static String encodePrettily(Object obj) throws EncodeException {
        try {
            return prettyMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new EncodeException("Failed to encode as JSON: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T decodeValue(String str, Class<?> clazz) throws DecodeException {
        try {
            return (T) mapper.readValue(str, clazz);
        } catch (Exception e) {
            throw new DecodeException("Failed to decode:" + e.getMessage());
        }
    }

}
