package org.rakam.report;

import com.facebook.presto.jdbc.PrestoDriver;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.rakam.kume.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 01:57.
 */
@Singleton
public class JdbcPool {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);
    private final PrestoConfig config;
    private final PrestoDriver driver = new PrestoDriver();

    private Map<String, Connection> jdbcDrivers = Maps.newConcurrentMap();

    private final Properties info;

    @Inject
    public JdbcPool(PrestoConfig config) {
        this.config = config;

        info = new Properties();
        info.setProperty("user", "Rakam");
    }

    public Connection getDriver(String url) {
        Connection conn = jdbcDrivers.get(url);

        if(conn!=null)
            return conn;

        LOGGER.debug("Connecting to driver {}", url);
        try {
            return DriverManager.getConnection(url, "root", "root");
        } catch (SQLException e) {
            LOGGER.error(format("Couldn't connect requested server {}", url), e);
            throw Throwables.propagate(e);
        }
    }

    public Connection getPrestoDriver() {
        try {
            Properties info = new Properties();
            info.setProperty("user", "Rakam"+new Random().nextInt());
            return DriverManager.getConnection("jdbc:presto://" + config.getAddress(), info);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
