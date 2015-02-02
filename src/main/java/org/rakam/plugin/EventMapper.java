package org.rakam.plugin;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by buremba on 26/05/14.
 */
public interface EventMapper {
    void map(ObjectNode event);
}