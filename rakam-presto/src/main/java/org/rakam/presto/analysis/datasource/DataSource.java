package org.rakam.presto.analysis.datasource;

import com.facebook.presto.jdbc.internal.guava.base.Function;
import com.facebook.presto.rakam.externaldata.DataManager;
import com.facebook.presto.rakam.externaldata.source.MysqlDataSource;
import com.facebook.presto.rakam.externaldata.source.PostgresqlDataSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import org.rakam.util.JsonHelper;

import javax.annotation.Nullable;

import java.util.Optional;

public interface DataSource
{
    ImmutableMap<String, Function<Object, DataManager.DataSourceFactory>> MAPPER = ImmutableMap.<String, Function<Object, DataManager.DataSourceFactory>>builder()
            .put(MysqlDataSource.NAME, new Function<Object, DataManager.DataSourceFactory>()
            {
                @Nullable
                @Override
                public DataManager.DataSourceFactory apply(@Nullable Object options)
                {
                    return JsonHelper.convert(options, MysqlDataSource.MysqlDataSourceFactory.class);
                }
            })
            .put(PostgresqlDataSource.NAME, new Function<Object, DataManager.DataSourceFactory>()
            {
                @Nullable
                @Override
                public DataManager.DataSourceFactory apply(@Nullable Object options)
                {
                    return JsonHelper.convert(options, PostgresqlDataSource.PostgresqlDataSourceFactory.class);
                }
            }).build();

    static DataManager.DataSourceFactory createDataSource(String type, Object options)
    {
        Function<Object, DataManager.DataSourceFactory> func = MAPPER.get(type);
        if (func == null) {
            throw new IllegalStateException("Unknown data source type");
        }
        return func.apply(options);
    }

    @JsonIgnore
    boolean isFile();

    Optional<String> test();
}
