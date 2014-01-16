package org.rakam.database.cassandra.filter;

import java.util.Map;

/**
 * Created by buremba on 21/12/13.
 */

public interface Filter {
    public Map<?, ?> filter(Map<?, ?> row);
}
