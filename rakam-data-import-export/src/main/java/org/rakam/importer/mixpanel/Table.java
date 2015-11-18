package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.SchemaField;

import java.util.Map;

public class Table {
    public final String rakamCollection;
    public final Map<String, SchemaField> mapping;

    @JsonCreator
    public Table(@JsonProperty("collection") String rakamCollection,
                 @JsonProperty("mapping") Map<String, SchemaField> mapping) {
        this.rakamCollection = rakamCollection;
        this.mapping = mapping;
    }
}
