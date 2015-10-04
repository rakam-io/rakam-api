package org.rakam.analysis;

import com.zaxxer.hikari.HikariDataSource;
import org.rakam.plugin.JDBCConfig;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class JDBCPoolDataSource extends HikariDataSource {
    private static final Map<String, JDBCPoolDataSource> pools = new ConcurrentHashMap<>();

    private JDBCPoolDataSource(JDBCConfig config) {
        checkArgument(config.getUrl() != null, "JDBC url is required");
        setJdbcUrl(config.getUrl());
        setUsername(config.getUsername());

        setPassword(config.getPassword());
        String scheme = URI.create(config.getUrl().substring(5)).getScheme();

        try {
            Class.forName(getClassName(scheme));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("JDBC driver is not installed.");
        }

        if (config.getMaxConnection() != null) {
            setMaximumPoolSize(config.getMaxConnection());
        }

        setAutoCommit(true);
        setPoolName("generic-jdbc-query-executor");
    }

    private String getClassName(String scheme) {
        switch (scheme) {
            case "postgresql":
                checkState(org.postgresql.Driver.isRegistered());
                return "org.postgresql.ds.PGSimpleDataSource";
            default:
                throw new IllegalArgumentException("Currently, only Postgresql JDBC adapter is supported.");
        }
    }

    public static JDBCPoolDataSource getOrCreateDataSource(JDBCConfig config) {
        return pools.computeIfAbsent(config.getUrl(),
                key -> new JDBCPoolDataSource(config));
    }
}
