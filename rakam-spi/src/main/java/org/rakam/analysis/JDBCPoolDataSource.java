package org.rakam.analysis;

import com.google.common.base.Throwables;
import org.rakam.plugin.JDBCConfig;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.sql.DataSource;
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

//        dataSource.setDatabase(config.getDatabase());
//        dataSource.setUser(config.getUsername());
//        dataSource.setPassword(config.getPassword());
//        dataSource.setHost(config.getHost());
//        dataSource.setApplicationName("rakam");
//        dataSource.setPort(config.getPort());
//
//        URI dbUri;
//        try {
//            dbUri = new URI(config.getUrl());
//        } catch (URISyntaxException e) {
//            throw Throwables.propagate(e);
//        }
//
//        Properties properties = new Properties();
//
//        Map<String, String> map = Splitter.on('&').trimResults().withKeyValueSeparator("=").split(dbUri.getQuery());
//        properties.putAll(map);
//
//        HikariConfig hikariConfig = new HikariConfig();
//        hikariConfig.setDataSource(dataSource);
//        hikariConfig.setDataSourceProperties(properties);
//
//        try {
//            Class.forName(getClassName(scheme));
//        } catch (ClassNotFoundException e) {
//            throw new IllegalStateException("JDBC driver is not installed.");
//        }
//
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
        if(config.getDataSource() == null) {
            return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
        } else {
            try {
                DataSource o = (DataSource) Class.forName(config.getDataSource()).newInstance();
                return o.getConnection();
            } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
