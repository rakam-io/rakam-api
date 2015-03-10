package org.rakam.report.metadata;


import com.facebook.presto.jdbc.PrestoDriver;
import com.facebook.presto.jdbc.internal.client.StatementClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/03/15 22:52.
 */
public class PrestoCLI {
    public PrestoCLI() throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        PrestoDriver prestoDriver = new PrestoDriver();
        Properties properties = new Properties();
        properties.put("user", "Rakam");

        Connection connect;
        try {
            connect = prestoDriver.connect("jdbc:presto://127.0.0.1:8080", properties);
        } catch (SQLException e) {
            return;
        }

        Class<? extends Connection> aClass = connect.getClass();
        Method startQuery = aClass.getDeclaredMethod("startQuery", String.class);
        startQuery.setAccessible(true);
        StatementClient invoke = (StatementClient) startQuery.invoke(connect, "select 1");
        invoke.advance();
        invoke.current().toString();

    }

}
