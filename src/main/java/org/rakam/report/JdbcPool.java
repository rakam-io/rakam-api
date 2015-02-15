package org.rakam.report;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.rakam.kume.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 01:57.
 */
@Singleton
public class JdbcPool {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    private Map<String, Connection> jdbcDrivers = Maps.newConcurrentMap();

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
}
