package org.rakam.plugin.user;

import org.rakam.report.QueryExecution;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:32.
 */
public interface UserStorage {
    public void handleCollect(String userJson);
    public QueryExecution executeQuery(String query);
}
