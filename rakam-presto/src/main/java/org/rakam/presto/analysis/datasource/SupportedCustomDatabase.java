package org.rakam.presto.analysis.datasource;

import com.facebook.presto.jdbc.internal.guava.base.Function;
import com.facebook.presto.rakam.externaldata.DataManager;
import com.facebook.presto.rakam.externaldata.source.MysqlDataSource;
import com.facebook.presto.rakam.externaldata.source.PostgresqlDataSource;
import org.postgresql.Driver;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;

public enum SupportedCustomDatabase
{
    POSTGRESQL(PostgresqlDataSource.PostgresqlDataSourceFactory.class, new Function<PostgresqlDataSource.PostgresqlDataSourceFactory, Optional<String>>()
    {
        @Nullable
        @Override
        public Optional<String> apply(@Nullable PostgresqlDataSource.PostgresqlDataSourceFactory factory)
        {
            try {
                Connection connect = new Driver().connect(
                        format("jdbc:postgresql://%s:%s/%s",
                                factory.getHost(),
                                Optional.ofNullable(factory.getPort()).orElse(5432),
                                factory.getDatabase()), null);
                String schemaPattern = connect.getSchema() == null ? "public" : factory.getSchema();
                ResultSet schemas = connect.getMetaData().getSchemas(null, schemaPattern);
                return schemas.next() ? Optional.empty() : Optional.of(format("Schema '%s' does not exist", schemaPattern));
            }
            catch (SQLException e) {
                return Optional.of(e.getMessage());
            }
        }
    }),
    MYSQL(MysqlDataSource.MysqlDataSourceFactory.class, new Function<MysqlDataSource.MysqlDataSourceFactory, Optional<String>>()
    {
        @Nullable
        @Override
        public Optional<String> apply(@Nullable MysqlDataSource.MysqlDataSourceFactory factory)
        {
            Properties info = new Properties();
            info.put("PGDBNAME", factory.getDatabase());
            info.put("PGPORT", factory.getPort());
            info.put("PGHOST", factory.getHost());

            try {
                Connection connect = new Driver().connect("jdbc:postgresql:", info);
                String schemaPattern = connect.getSchema() == null ? "public" : connect.getSchema();
                ResultSet schemas = connect.getMetaData().getSchemas(null, schemaPattern);
                return schemas.next() ? Optional.empty() : Optional.of(format("Schema '%s' does not exist", schemaPattern));
            }
            catch (SQLException e) {
                return Optional.of(e.getMessage());
            }
        }
    });

    private final Class<? extends DataManager.DataSourceFactory> factoryClass;
    private final Function<? extends DataManager.DataSourceFactory, Optional<String>> testFunction;

    SupportedCustomDatabase(Class<? extends DataManager.DataSourceFactory> factoryClass, Function<? extends DataManager.DataSourceFactory, Optional<String>> testFunction)
    {
        this.factoryClass = factoryClass;
        this.testFunction = testFunction;
    }

    public static Function<? extends DataManager.DataSourceFactory, Optional<String>> getTestFunction(String value) {
        for (SupportedCustomDatabase database : values()) {
            if (database.name().equals(value)) {
                return database.testFunction;
            }
        }
        throw new IllegalStateException();
    }
}
