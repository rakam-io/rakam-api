package org.rakam.presto.analysis.datasource;

import com.facebook.presto.hadoop.$internal.com.google.common.base.Throwables;
import com.facebook.presto.rakam.externaldata.DataManager;
import com.facebook.presto.rakam.externaldata.JDBCSchemaConfig;
import com.facebook.presto.rakam.externaldata.source.MysqlDataSource.MysqlDataSourceFactory;
import com.facebook.presto.rakam.externaldata.source.PostgresqlDataSource.PostgresqlDataSourceFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;

public enum SupportedCustomDatabase
{
    POSTGRESQL(PostgresqlDataSourceFactory.class, new CDataSource<PostgresqlDataSourceFactory>()
    {
        @Override
        public Optional<String> test(PostgresqlDataSourceFactory factory)
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
        public Connection openConnection(PostgresqlDataSourceFactory factory)
                throws SQLException
        {
            return new org.postgresql.Driver().connect(
                    format("jdbc:postgresql://%s:%s/%s",
                            factory.getHost(),
                            Optional.ofNullable(factory.getPort()).orElse(5432),
                            factory.getDatabase()), null);
        }
    }),
    MYSQL(MysqlDataSourceFactory.class, new CDataSource<MysqlDataSourceFactory>()
    {
        @Override
        public Optional<String> test(MysqlDataSourceFactory factory)
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
                        throw com.google.common.base.Throwables.propagate(e);
                    }
                }
            }
        }

        @Override
        public Connection openConnection(MysqlDataSourceFactory factory)
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

    private final CDataSource testFunction;
    private final Class<? extends JDBCSchemaConfig> factoryClass;

    SupportedCustomDatabase(Class<? extends JDBCSchemaConfig> factoryClass, CDataSource testFunction)
    {
        this.factoryClass = factoryClass;
        this.testFunction = testFunction;
    }

    public Class<? extends JDBCSchemaConfig> getFactoryClass()
    {
        return factoryClass;
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
        throw new IllegalStateException();
    }

    public interface CDataSource<T extends DataManager.DataSourceFactory>
    {
        Optional<String> test(T data);

        Connection openConnection(T data)
                throws SQLException;
    }
}
