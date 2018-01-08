package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.rakam.collection.FieldType;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;

public class Parameter {
    public final FieldType type;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final String placeholder;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final boolean required;
    public final String description;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final List<Choice> choices;
    public final boolean hidden;
    public Object value;

    @JsonCreator
    public Parameter(
            @ApiParam("type") FieldType type,
            @ApiParam(value = "value", required = false) Object value,
            @ApiParam(value = "required", required = false) Boolean required,
            @ApiParam(value = "placeholder", required = false) String placeholder,
            @ApiParam(value = "choices", required = false) List<Choice> choices,
            @ApiParam(value = "description", required = false) String description,
            @ApiParam(value = "hidden", required = false) Boolean hidden) {
        this.type = type;
        this.required = Boolean.TRUE.equals(required);
        this.value = value;
        this.placeholder = placeholder;
        this.choices = choices;
        this.description = description;
        this.hidden = Boolean.TRUE.equals(hidden);
    }

    public static class Choice {
        public final String key;
        public final String value;

        @JsonCreator
        public Choice(@ApiParam("key") String key, @ApiParam("value") String value) {
            this.key = key;
            this.value = value;
        }
    }
}
