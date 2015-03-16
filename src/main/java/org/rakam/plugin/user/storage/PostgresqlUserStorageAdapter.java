package org.rakam.plugin.user.storage;

import com.google.inject.Singleton;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryExecution;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:46.
 */
@Singleton
public class PostgresqlUserStorageAdapter implements UserStorage {
    @Override
    public void handleCollect(String userJson) {

    }

    @Override
    public QueryExecution executeQuery(String query) {
        return null;
    }
}
