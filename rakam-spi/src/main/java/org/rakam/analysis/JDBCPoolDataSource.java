package org.rakam.analysis;

import org.rakam.plugin.JDBCConfig;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class JDBCPoolDataSource implements ConnectionFactory {
    private static final Map<String, JDBCPoolDataSource> pools = new ConcurrentHashMap<>();
    private final JDBCConfig config;

    private JDBCPoolDataSource(JDBCConfig config) {
        checkArgument(config.getUrl() != null, "JDBC url is required");
        this.config = config;
//        new HikariDataSource();
//        setJdbcUrl(config.getUrl());
//        setUsername(config.getUsername());
//
//        setPassword(config.getPassword());
//        setConnectionTestQuery("select 1");
//        setConnectionTimeout(10000);
//        String scheme = URI.create(config.getUrl().substring(5)).getScheme();
//
//        try {
//            Class.forName(getClassName(scheme));
//        } catch (ClassNotFoundException e) {
//            throw new IllegalStateException("JDBC driver is not installed.");
//        }

//        if (config.getMaxConnection() != null) {
//            setMaximumPoolSize(config.getMaxConnection());
//        }
//
//        setAutoCommit(true);
//        setPoolName("generic-jdbc-query-executor");
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

    @Override
    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
    }
}
