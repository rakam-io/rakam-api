package org.rakam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/07/15 22:51.
 */
public class SystemRegistry {
    private final List<Module> modules;

    public SystemRegistry(List<Module> modules) {
        this.modules = Collections.unmodifiableList(modules);
    }

    public List<Module> getModules() {
        return modules;
    }

    public static class Module {
        public final String name;
        public final String description;
        public final String className;

        @JsonCreator
        public Module(@JsonProperty("name") String name,
                      @JsonProperty("description") String description,
                      @JsonProperty("className") String className) {
            this.name = name;
            this.description = description;
            this.className = className;
        }
    }
}
