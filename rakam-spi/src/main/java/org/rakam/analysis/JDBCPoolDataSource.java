package org.rakam.analysis;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.rakam.plugin.JDBCConfig;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class JDBCPoolDataSource implements DataSource {
    private static final Map<JDBCConfig, JDBCPoolDataSource> pools = new ConcurrentHashMap<>();
    private final JDBCConfig config;
    private final HikariDataSource dataSource;

    private JDBCPoolDataSource(JDBCConfig config) {
        checkArgument(config.getUrl() != null, "JDBC url is required");
        this.config = config;


        URI dbUri;
        try {
            dbUri = new URI(config.getUrl().substring(5));
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }

        Properties properties = new Properties();

        Map<String, String> map;
        if(dbUri.getQuery() != null) {
            map = Splitter.on('&').trimResults()
                    .withKeyValueSeparator("=")
                    .split(dbUri.getQuery());
        } else {
            map = ImmutableMap.of();
        }

        properties.putAll(map);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setJdbcUrl(config.getUrl());
        hikariConfig.setDataSourceProperties(properties);

        if (config.getMaxConnection() != null) {
            hikariConfig.setMaximumPoolSize(config.getMaxConnection());
        }

        hikariConfig.setAutoCommit(true);
        hikariConfig.setPoolName("generic-jdbc-query-executor");
        dataSource = new HikariDataSource(hikariConfig);
    }

    private void assureLoaded(String scheme) {
        switch (scheme) {
            case "postgresql":
                checkState(org.postgresql.Driver.isRegistered());
            case "pgsql":
                try {
                    Class.forName("com.impossibl.postgres.jdbc.PGDriver");
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("Driver doesn't exist in classpath");
                }
            default:
                throw new IllegalArgumentException("Currently, only Postgresql JDBC adapter is supported.");
        }
    }

    public static JDBCPoolDataSource getOrCreateDataSource(JDBCConfig config) {
        return pools.computeIfAbsent(config,
                key -> new JDBCPoolDataSource(config));
    }

    @Override
    public Connection getConnection() throws SQLException {
//        if(config.getDataSource() == null) {
//            return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
//        } else {
//            try {
//                DataSource o = (DataSource) Class.forName(config.getDataSource()).newInstance();
//
//                return o.getConnection();
//            } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
//                throw Throwables.propagate(e);
//            }
//        }
          return dataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
//        if(config.getDataSource() == null) {
//            return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
//        } else {
//            try {
//                DataSource o = (DataSource) Class.forName(config.getDataSource()).newInstance();
//
//                return o.getConnection();
//            } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
//                throw Throwables.propagate(e);
//            }
//        }
          return dataSource.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        dataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        dataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return dataSource.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return dataSource.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return dataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return dataSource.isWrapperFor(iface);
    }
}
