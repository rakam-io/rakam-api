package org.rakam.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 12:14.
 */
@Singleton
public class RakamConfig {
    @JsonDeserialize(using = RakamConfigJsonDeserializer.class) private List<RakamPlugin> plugins;

    public List<RakamPlugin> getPlugins() {
        return Collections.unmodifiableList(plugins);
    }

    public static class RakamConfigJsonDeserializer extends JsonDeserializer<List<RakamPlugin>> {

        @Override
        public List<RakamPlugin> deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
            ArrayNode root = jp.readValueAsTree();

            List<RakamPlugin> plugins = new ArrayList<>();

            for (JsonNode jsonNode : root) {
                String pluginClass = jsonNode.asText();

                try {
                    Class<?> clazz = Class.forName((String) pluginClass);
                    plugins.add((RakamPlugin) clazz.newInstance());
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("plugin class couldn't found", e);
                } catch (InstantiationException e) {
                    throw new IllegalStateException("plugin class does not have empty constructor", e);
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("plugin class does not have public empty constructor", e);
                } catch (ClassCastException e) {
                    throw new IllegalStateException("plugin class does not implement RakamPlugin", e);
                }
            }

            return plugins;
        }
    }
}
