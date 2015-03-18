package org.rakam.plugin.user.storage;

import org.rakam.plugin.user.FilterCriteria;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:32.
 */
public interface UserStorage {
    public void create(String project, Map<String, Object> properties);
    public QueryResult filter(String project, List<FilterCriteria> filters, int limit, int offset);
    public List<Column> getMetadata(String project);
}
