package org.rakam.plugin.user;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;

public interface ISingleUserBatchOperation {
    ObjectNode getSetProperties();

    ObjectNode getSetPropertiesOnce();

    Map<String, Double> getIncrementProperties();

    List<String> getUnsetProperties();

    Long getTime();

    Object getUser();
}
