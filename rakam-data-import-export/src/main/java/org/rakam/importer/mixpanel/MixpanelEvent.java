package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class MixpanelEvent {
    public final String event;
    public final Map<String, Object> properties;

    @JsonCreator
    public MixpanelEvent(@JsonProperty("event") String event,
                         @JsonProperty("properties") Map<String, Object> properties) {
        this.event = event;
        this.properties = properties;
    }
}
