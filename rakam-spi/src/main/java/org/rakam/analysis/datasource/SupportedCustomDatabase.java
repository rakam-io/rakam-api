package org.rakam.analysis.datasource;

import com.google.common.base.Throwables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;

public enum SupportedCustomDatabase
{
    POSTGRESQL(new CDataSource<JDBCSchemaConfig>()
    {
        @Override
        public Optional<String> test(JDBCSchemaConfig factory)
        {
            Connection connect = null;
            try {
                connect = openConnection(factory);
                String schemaPattern = connect.getSchema() == null ? "public" : factory.getSchema();
                ResultSet schemas = connect.getMetaData().getSchemas(null, schemaPattern);
                return schemas.next() ? Optional.empty() : Optional.of(format("Schema '%s' does not exist", schemaPattern));
            }
            catch (SQLException e) {
                return Optional.of(e.getMessage());
            }
            finally {
                if (connect != null) {
                    try {
                        connect.close();
                    }
                    catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }

        @Override
        public Connection openConnection(JDBCSchemaConfig factory)
                throws SQLException
        {
            Properties properties = new Properties();
            properties.setProperty("user", factory.getUsername());
            properties.setProperty("password", factory.getPassword());

            return new org.postgresql.Driver().connect(
                    format("jdbc:postgresql://%s:%s/%s",
                            factory.getHost(),
                            Optional.ofNullable(factory.getPort()).orElse(5432),
                            factory.getDatabase()), properties);
        }
    }),
    MYSQL(new CDataSource<JDBCSchemaConfig>()
    {
        @Override
        public Optional<String> test(JDBCSchemaConfig factory)
        {
            Connection connect = null;
            try {
                connect = openConnection(factory);
                ResultSet schemas = connect.getMetaData().getSchemas(null, null);
                return schemas.next() ? Optional.empty() : Optional.empty();
            }
            catch (SQLException e) {
                return Optional.of(e.getMessage());
            }
            finally {
                if (connect != null) {
                    try {
                        connect.close();
                    }
                    catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }

        @Override
        public Connection openConnection(JDBCSchemaConfig factory)
                throws SQLException
        {
            Properties info = new Properties();
            Optional.ofNullable(factory.getUsername()).map(value -> info.put("user", value));
            Optional.ofNullable(factory.getPassword()).map(value -> info.put("password", value));

            return new com.mysql.jdbc.Driver().connect(format("jdbc:mysql://%s:%s/%s",
                    factory.getHost(),
                    Optional.ofNullable(factory.getPort()).orElse(3306),
                    factory.getDatabase()), info);
        }
    });

    private final CDataSource<JDBCSchemaConfig> testFunction;

    SupportedCustomDatabase(CDataSource<JDBCSchemaConfig> testFunction)
    {
        this.testFunction = testFunction;
    }

    public CDataSource getTestFunction()
    {
        return testFunction;
    }

    public static SupportedCustomDatabase getAdapter(String value)
    {
        for (SupportedCustomDatabase database : values()) {
            if (database.name().equals(value)) {
                return database;
            }
        }
        throw new IllegalArgumentException();
    }

    public interface CDataSource<T>
    {
        Optional<String> test(T data);

        Connection openConnection(T data)
                throws SQLException;
    }
}
