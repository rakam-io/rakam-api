package org.rakam.presto.analysis.datasource;

import com.facebook.presto.jdbc.internal.guava.base.Function;
import com.facebook.presto.rakam.externaldata.DataManager.DataSourceFactory;
import com.facebook.presto.rakam.externaldata.source.MysqlDataSource;
import com.facebook.presto.rakam.externaldata.source.MysqlDataSource.MysqlDataSourceFactory;
import com.facebook.presto.rakam.externaldata.source.PostgresqlDataSource;
import com.facebook.presto.rakam.externaldata.source.PostgresqlDataSource.PostgresqlDataSourceFactory;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.Column;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.postgresql.Driver;
import org.rakam.collection.FieldType;
import org.rakam.presto.analysis.PrestoMetastore.SignatureReferenceType;
import org.rakam.presto.analysis.PrestoQueryExecution;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.ExternalSourceType.AVRO;
import static com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.ExternalSourceType.CSV;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.Boolean.FALSE;
import static org.rakam.presto.analysis.PrestoMetastore.toSql;

public class CustomDataSource<T extends DataSourceFactory>
{
    static {
        JsonHelper.getMapper().registerModule(new SimpleModule()
                .addDeserializer(Type.class, new JsonDeserializer<Type>()
                {
                    @Override
                    public Type deserialize(JsonParser jp, DeserializationContext ctxt)
                            throws IOException
                    {
                        return new SignatureReferenceType(parseTypeSignature(toSql(FieldType.fromString(jp.getValueAsString()))), null);
                    }
                }).addSerializer(Type.class, new JsonSerializer<Type>()
                {
                    @Override
                    public void serialize(Type value, JsonGenerator jgen, SerializerProvider provider)
                            throws IOException
                    {
                        jgen.writeString(PrestoQueryExecution.fromPrestoType(value.getTypeSignature().getBase(),
                                value.getTypeParameters().stream()
                                        .map(argument -> argument.getTypeSignature().getBase()).iterator()).name());
                    }
                }));
    }

    public static enum SupportedCustomDatabase
    {
        POSTGRESQL(PostgresqlDataSourceFactory.class, new Function<PostgresqlDataSourceFactory, Optional<String>>()
        {
            @Nullable
            @Override
            public Optional<String> apply(@Nullable PostgresqlDataSourceFactory factory)
            {
                Properties info = new Properties();
                info.put("PGDBNAME", factory.getDatabase());
                info.put("PGPORT", factory.getPort());
                info.put("PGHOST", factory.getHost());

                try {
                    Connection connect = new Driver().connect("jdbc:postgresql:", info);
                    String schemaPattern = connect.getSchema() == null ? "public" : connect.getSchema();
                    ResultSet schemas = connect.getMetaData().getSchemas(null, schemaPattern);
                    return schemas.next() ? Optional.empty() : Optional.of(String.format("Schema '%s' does not exist", schemaPattern));
                }
                catch (SQLException e) {
                    return Optional.of(e.getMessage());
                }
            }
        }),
        MYSQL(MysqlDataSourceFactory.class, new Function<MysqlDataSourceFactory, Optional<String>>()
        {
            @Nullable
            @Override
            public Optional<String> apply(@Nullable MysqlDataSourceFactory factory)
            {
                Properties info = new Properties();
                info.put("PGDBNAME", factory.getDatabase());
                info.put("PGPORT", factory.getPort());
                info.put("PGHOST", factory.getHost());

                try {
                    Connection connect = new Driver().connect("jdbc:postgresql:", info);
                    String schemaPattern = connect.getSchema() == null ? "public" : connect.getSchema();
                    ResultSet schemas = connect.getMetaData().getSchemas(null, schemaPattern);
                    return schemas.next() ? Optional.empty() : Optional.of(String.format("Schema '%s' does not exist", schemaPattern));
                }
                catch (SQLException e) {
                    return Optional.of(e.getMessage());
                }
            }
        });

        private final Class<? extends DataSourceFactory> factoryClass;
        private final Function<? extends DataSourceFactory, Optional<String>> testFunction;

        SupportedCustomDatabase(Class<? extends DataSourceFactory> factoryClass, Function<? extends DataSourceFactory, Optional<String>> testFunction)
        {
            this.factoryClass = factoryClass;
            this.testFunction = testFunction;
        }

        public static Function<? extends DataSourceFactory, Optional<String>> getTestFunction(String value) {
            for (SupportedCustomDatabase database : values()) {
                if (database.name().equals(value)) {
                    return database.testFunction;
                }
            }
            throw new IllegalStateException();
        }
    }

    public final String schemaName;
    public final String type;
    public final T options;

    @JsonCreator
    public CustomDataSource(
            @ApiParam("type") String type,
            @ApiParam("schemaName") String schemaName,
            @ApiParam("options") T options)
    {
        this.schemaName = schemaName;
        this.options = options;
        this.type = type;
    }

    public interface DataSource
    {
        ImmutableMap<String, Function<Object, DataSourceFactory>> MAPPER = ImmutableMap.<String, Function<Object, DataSourceFactory>>builder()
                .put(MysqlDataSource.NAME, new Function<Object, DataSourceFactory>()
                {
                    @Nullable
                    @Override
                    public DataSourceFactory apply(@Nullable Object options)
                    {
                        return JsonHelper.convert(options, MysqlDataSourceFactory.class);
                    }
                })
                .put(PostgresqlDataSource.NAME, new Function<Object, DataSourceFactory>()
                {
                    @Nullable
                    @Override
                    public DataSourceFactory apply(@Nullable Object options)
                    {
                        return JsonHelper.convert(options, PostgresqlDataSourceFactory.class);
                    }
                }).build();

        static DataSourceFactory createDataSource(String type, Object options)
        {
            Function<Object, DataSourceFactory> func = MAPPER.get(type);
            if (func == null) {
                throw new IllegalStateException("Unknown data source type");
            }
            return func.apply(options);
        }

        @JsonIgnore
        boolean isFile();

        Optional<String> test();
    }

    public static class ExternalFileCustomDataSource
            implements DataSource
    {
        private final RemoteFileDataSource.RemoteTable remoteTable;

        public ExternalFileCustomDataSource(RemoteFileDataSource.RemoteTable remoteTable)
        {
            this.remoteTable = new RemoteFileDataSource.RemoteTable(
                    remoteTable.name, remoteTable.url,
                    remoteTable.indexUrl, remoteTable.typeOptions,
                    fillColumnIfNotSet(remoteTable.columns), remoteTable.compressionType, remoteTable.format);
        }

        @Override
        public boolean isFile()
        {
            return true;
        }

        private List<Column> fillColumnIfNotSet(List<Column> columns)
        {
            if (columns != null) {
                return columns;
            }

            if (columns == null && remoteTable.format == CSV && !FALSE.toString().equals(remoteTable.typeOptions.get("use_header"))) {
                URL file;
                try {
                    file = getFile();
                }
                catch (RuntimeException e) {
                    throw new RakamException(e.getMessage(), BAD_REQUEST);
                }

                String separator = Optional.ofNullable(remoteTable.typeOptions.get("column_separator")).orElse(",");

                try {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file.openStream()));

                    ImmutableList.Builder<Column> builder = ImmutableList.builder();
                    for (String column : Splitter.on(separator).split(bufferedReader.readLine())) {
                        builder.add(new Column(column, VarcharType.VARCHAR));
                    }

                    return builder.build();
                }
                catch (IOException e) {
                    throw new RakamException("Error while parsing CSV: " + e.getMessage(), BAD_REQUEST);
                }
            }

            throw new RakamException("columns parameter is required", BAD_REQUEST);
        }

        private URL getFile()
                throws RuntimeException
        {
            if (!remoteTable.url.getProtocol().equals("http") && !remoteTable.url.getProtocol().equals("https") && !remoteTable.url.getProtocol().equals("ftp")) {
                throw new RuntimeException("URL is not valid. Use http, https or ftp schemes");
            }

            URL testUrl = remoteTable.url;
            if (remoteTable.indexUrl) {
                List<String> urls;
                try {
                    urls = JsonHelper.read(ByteStreams.toByteArray(remoteTable.url.openStream()),
                            new TypeReference<List<String>>() {});
                }
                catch (IOException e) {
                    throw new RuntimeException("The index file must be an array containing urls. Example: [\"http://myurl.com/a.csv\"]");
                }
                if (urls == null || urls.isEmpty()) {
                    throw new RuntimeException("Index file doesn't have any entry");
                }

                try {
                    testUrl = new URL(urls.get(0));
                }
                catch (MalformedURLException e) {
                    throw new RuntimeException("Index file doesn't contain URL values");
                }
            }

            return testUrl;
        }

        @Override
        public Optional<String> test()
        {
            try {
                URL testUrl;
                try {
                    testUrl = getFile();
                }
                catch (RuntimeException e) {
                    return Optional.of(e.getMessage());
                }

                if (remoteTable.format == CSV) {
                    String line;
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(testUrl.openStream()));
                    int i = 0;
                    try {
                        while (((line = bufferedReader.readLine()) != null) && i < 5) {
                            if (line.isEmpty()) {
                                // TODO: better alternative to detect csv
                                return Optional.of("There are empty lines");
                            }
                            i++;
                        }
                    }
                    finally {
                        bufferedReader.close();
                    }
                }
                else if (remoteTable.format == AVRO) {
                    try {
                        testUrl.openStream().read();
                    }
                    catch (IOException e) {
                        return Optional.of("Unable to read data from server");
                    }
                }
                else {
                    throw new IllegalStateException();
                }

                return Optional.empty();
            }
            catch (IOException e) {
                return Optional.of(e.getMessage());
            }
        }
    }


}
