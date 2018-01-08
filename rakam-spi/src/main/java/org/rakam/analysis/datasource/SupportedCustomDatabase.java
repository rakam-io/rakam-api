package org.rakam.analysis.datasource;

import com.google.common.base.Throwables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;

public enum SupportedCustomDatabase {
    POSTGRESQL(new CDataSource<JDBCSchemaConfig>() {
        @Override
        public Optional<String> test(JDBCSchemaConfig factory) {
            Connection connect = null;
            try {
                connect = openConnection(factory);
                String schemaPattern = connect.getSchema() == null ? "public" : factory.getSchema();
                ResultSet schemas = connect.getMetaData().getSchemas(null, schemaPattern);
                return schemas.next() ? Optional.empty() : Optional.of(format("Schema '%s' does not exist", schemaPattern));
            } catch (SQLException e) {
                return Optional.of(e.getMessage());
            } finally {
                if (connect != null) {
                    try {
                        connect.close();
                    } catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }

        @Override
        public Connection openConnection(JDBCSchemaConfig factory)
                throws SQLException {
            Properties properties = new Properties();
            Optional.ofNullable(factory.getPassword())
                    .ifPresent(pass -> properties.setProperty("password", pass));
            Optional.ofNullable(factory.getUsername())
                    .ifPresent(user -> properties.setProperty("user", user));
            properties.setProperty("loginTimeout", "120");
            properties.setProperty("socketTimeout", "120");
            properties.setProperty("connectTimeout", "120");
            if (factory.getEnableSSL() != null) {
                properties.setProperty("ssl", factory.getEnableSSL().toString());
            }

            return new org.postgresql.Driver().connect(
                    format("jdbc:postgresql://%s:%s/%s",
                            factory.getHost(),
                            Optional.ofNullable(factory.getPort()).orElse(5432),
                            factory.getDatabase()), properties);
        }
    }),
    REDSHIFT(new CDataSource<JDBCSchemaConfig>() {
        @Override
        public Optional<String> test(JDBCSchemaConfig factory) {
            Connection connect = null;
            try {
                connect = openConnection(factory);
                String schemaPattern = connect.getSchema() == null ? "public" : factory.getSchema();
                ResultSet schemas = connect.getMetaData().getSchemas(null, schemaPattern);
                return schemas.next() ? Optional.empty() : Optional.of(format("Schema '%s' does not exist", schemaPattern));
            } catch (SQLException e) {
                return Optional.of(e.getMessage());
            } finally {
                if (connect != null) {
                    try {
                        connect.close();
                    } catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }

        @Override
        public Connection openConnection(JDBCSchemaConfig factory)
                throws SQLException {
            Properties properties = new Properties();
            Optional.ofNullable(factory.getPassword())
                    .ifPresent(pass -> properties.setProperty("password", pass));
            Optional.ofNullable(factory.getUsername())
                    .ifPresent(user -> properties.setProperty("user", user));
            properties.setProperty("loginTimeout", "120");
            properties.setProperty("socketTimeout", "120");
            properties.setProperty("connectTimeout", "120");
            if (factory.getEnableSSL() != null) {
                properties.setProperty("ssl", factory.getEnableSSL().toString());
            }
            properties.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");

            return DriverManager.getConnection(
                    format("jdbc:postgresql://%s:%s/%s",
                            factory.getHost(),
                            Optional.ofNullable(factory.getPort()).orElse(5432),
                            factory.getDatabase()), properties);
        }
    }),
    MYSQL(new CDataSource<JDBCSchemaConfig>() {
        @Override
        public Optional<String> test(JDBCSchemaConfig factory) {
            Connection connect = null;
            try {
                connect = openConnection(factory);
                ResultSet schemas = connect.getMetaData().getSchemas(null, null);
                return schemas.next() ? Optional.empty() : Optional.empty();
            } catch (SQLException e) {
                return Optional.of(e.getMessage());
            } finally {
                if (connect != null) {
                    try {
                        connect.close();
                    } catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }

        @Override
        public Connection openConnection(JDBCSchemaConfig factory)
                throws SQLException {
            Properties info = new Properties();
            Optional.ofNullable(factory.getUsername()).map(value -> info.put("user", value));
            Optional.ofNullable(factory.getPassword()).map(value -> info.put("password", value));
            info.setProperty("socketTimeout", "120000");
            info.setProperty("connectTimeout", "120000");

            return new com.mysql.jdbc.Driver().connect(format("jdbc:mysql://%s:%s/%s",
                    factory.getHost(),
                    Optional.ofNullable(factory.getPort()).orElse(3306),
                    factory.getDatabase()), info);
        }
    }),
    MSSQL(new CDataSource<JDBCSchemaConfig>() {
        @Override
        public Optional<String> test(JDBCSchemaConfig config) {
            Connection connection = null;
            try {
                connection = openConnection(config);
                return Optional.empty();
            } catch (SQLException e) {
                return Optional.of(e.getMessage());
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }

        @Override
        public Connection openConnection(JDBCSchemaConfig config) throws SQLException {
            Properties info = new Properties();
            String userName = config.getUsername();
            String password = config.getPassword();
            Optional.ofNullable(userName).map(value -> info.put("user", value));
            Optional.ofNullable(password).map(value -> info.put("password", value));

            return new com.microsoft.sqlserver.jdbc.SQLServerDriver()
                    .connect(format("jdbc:sqlserver://%s:%s;databaseName=%s",
                            config.getHost(),
                            Optional.ofNullable(config.getPort()).orElse(1433),
                            config.getDatabase()), info);
        }
    });

    private final CDataSource<JDBCSchemaConfig> dataSource;

    SupportedCustomDatabase(CDataSource<JDBCSchemaConfig> dataSource) {
        this.dataSource = dataSource;
    }

    public static SupportedCustomDatabase getAdapter(String value) {
        for (SupportedCustomDatabase database : values()) {
            if (database.name().equals(value)) {
                return database;
            }
        }
        throw new IllegalArgumentException();
    }

    public CDataSource getDataSource() {
        return dataSource;
    }

    public interface CDataSource<T> {
        Optional<String> test(T data);

        Connection openConnection(T data)
                throws SQLException;
    }
}
