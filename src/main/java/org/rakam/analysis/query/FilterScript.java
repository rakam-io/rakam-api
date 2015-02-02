package org.rakam.analysis.query;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.util.json.JsonElement;

import java.io.Serializable;

/**
 * Created by buremba on 04/05/14.
 */
public interface FilterScript extends Serializable {
    public abstract boolean test(ObjectNode event);

    public abstract boolean requiresUser();

    public abstract JsonElement toJson();
}