package org.rakam.plugin.user.impl;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class JDBCUserStorageConfig {
    private List<String> columns;
    private String sessionColumn;
    private String lastSeenColumn;
    private Map<String, String> mappings;

    //    @Config("plugin.user.storage.jdbc.session_column")
//    public void setSessionColumn(String sessionColumn) {
//        this.sessionColumn = sessionColumn;
//    }
//
//    public String getSessionColumn() {
//        return sessionColumn;
//    }
//
//    @Config("plugin.user.storage.jdbc.last_seen_column")
//    public void setLastSeenColumnName(String lastLoginColumnName) {
//        this.lastSeenColumn = lastLoginColumnName;
//    }
//
//    public String getLastSeenColumnName() {
//        return lastSeenColumn;
//    }
//

    @Config("plugin.user.storage.mappings")
    public JDBCUserStorageConfig setMappings(String mappings) {
        this.mappings = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(
                        Splitter.on(',').omitEmptyStrings().trimResults().split(mappings).iterator(), Spliterator.ORDERED
                ), false)
                .collect(Collectors.toMap(mapping -> mapping.split("\\:", 2)[0], mapping -> mapping.split("\\:", 2)[1]));
        return this;
    }

    public Map<String, String> getMappings() {
        return mappings;
    }

    @Config("plugin.user.storage.columns")
    public void setColumns(String columns) {
        this.columns = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(columns));
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getLastSeenColumn() {
        return lastSeenColumn;
    }
}
