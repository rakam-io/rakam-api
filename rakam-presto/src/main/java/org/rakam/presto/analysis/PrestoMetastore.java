package org.rakam.presto.analysis;

import com.facebook.presto.jdbc.internal.client.ClientSession;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoMetastore extends AbstractMetastore {

    private final DBI dbi;
    private final MetadataDao dao;
    private final DBI reportDbi;
    private final PrestoConfig prestoConfig;
    private final ClientSession defaultSession;

    @Inject
    public PrestoMetastore(@Named("presto.metastore.jdbc") JDBCPoolDataSource prestoMetastoreDataSource,
                           @Named("report.metadata.store.jdbc") JDBCPoolDataSource reportDataSource,
                           EventBus eventBus, FieldDependencyBuilder.FieldDependency fieldDependency,
                           PrestoConfig prestoConfig) {
        super(fieldDependency, eventBus);
        dbi = new DBI(prestoMetastoreDataSource);
        dbi.registerMapper(new TableColumn.Mapper(new SignatureReferenceTypeManager()));
        this.dao = onDemandDao(dbi, MetadataDao.class);
        reportDbi = new DBI(reportDataSource);
        this.prestoConfig = prestoConfig;
        defaultSession = new ClientSession(
                prestoConfig.getAddress(),
                "rakam",
                "api-server",
                prestoConfig.getColdStorageConnector(),
                "default",
                TimeZone.getTimeZone(ZoneOffset.UTC).getID(),
                Locale.ENGLISH,
                ImmutableMap.<String, String>of(),
                null,
                false, new com.facebook.presto.jdbc.internal.airlift.units.Duration(1, TimeUnit.MINUTES));
    }

    @PostConstruct
    public void setup() {
        setupTables();
        super.checkExistingSchema();
    }

    private void setupTables() {
        reportDbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name TEXT NOT NULL,\n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) {
        return getOrCreateCollectionFields(project, collection, fields, 5);
    }

    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields, int tryCount) {
        if (!collection.matches("^[a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Only alphanumeric characters allowed in collection name.");
        }

        String query;
        List<SchemaField> schemaFields = getCollection(project, collection);
        List<SchemaField> lastFields;
        if (schemaFields.isEmpty() && dao.getTableInformation(project, collection) == null) {
            List<SchemaField> currentFields = new ArrayList<>();

            if (!getProjects().contains(project)) {
                throw new NotExistsException("project", UNAUTHORIZED);
            }
            String queryEnd = fields.stream()
                    .map(f -> {
                        currentFields.add(f);
                        return f;
                    })
                    .map(f -> format("\"%s\" %s", f.getName(), toSql(f.getType())))
                    .collect(Collectors.joining(", "));
            if (queryEnd.isEmpty()) {
                return currentFields;
            }
            query = format("CREATE TABLE %s.\"%s\".\"%s\" (%s) WITH (temporal_column = '_time') ", prestoConfig.getColdStorageConnector(), project, collection, queryEnd);
            QueryResult join = new PrestoQueryExecution(PrestoQueryExecutor.startQuery(query, defaultSession)).getResult().join();
            if (join.isFailed()) {
                if (join.getError().message.contains("exists") || join.getError().message.equals("Failed to perform metadata operation")) {
                    if(tryCount > 0) {
                        return getOrCreateCollectionFields(project, collection, fields, tryCount--);
                    } else {
                        throw new RakamException(String.format("Failed to add new fields to collection %s.%s: %s",
                                project, collection, Arrays.toString(fields.toArray())),
                                INTERNAL_SERVER_ERROR);
                    }
                } else {
                    throw new IllegalStateException("Error while executing query: " + join.getError());
                }
            }

            lastFields = schemaFields;
        } else {
            List<SchemaField> newFields = new ArrayList<>();

            fields.stream()
                    .filter(field -> schemaFields.stream().noneMatch(f -> f.getName().equals(field.getName())))
                    .forEach(f -> {
                        newFields.add(f);
                        String q = format("ALTER TABLE %s.\"%s\".\"%s\" ADD COLUMN \"%s\" %s",
                                prestoConfig.getColdStorageConnector(), project, collection,
                                f.getName(), toSql(f.getType()));
                        QueryResult join = new PrestoQueryExecution(PrestoQueryExecutor.startQuery(q, defaultSession)).getResult().join();
                        if (join.isFailed()) {
                            if (!join.getError().message.contains("exists")) {
                                throw new IllegalStateException("Error while executing query: " + join.getError());
                            }
                        }
                    });

            lastFields = ImmutableList.<SchemaField>builder().addAll(schemaFields).addAll(newFields).build();
        }

        super.onCreateCollection(project, collection, schemaFields);
        return lastFields;
    }

    @Override
    public Map<String, Set<String>> getAllCollections() {
        return getProjects().stream().collect(Collectors.toMap(a -> a, a -> getCollectionNames(a)));
    }


    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        return dao.listTableColumns(project, collection).stream().map(column -> {
            TypeSignature typeSignature = column.getDataType().getTypeSignature();

            return new SchemaField(column.getColumnName(), PrestoQueryExecution.fromPrestoType(typeSignature.getBase(),
                    typeSignature.getParameters().isEmpty() ? null : typeSignature.getParameters().get(0).getTypeSignature().getBase()));
        }).collect(Collectors.toList());
    }

    @Override
    public void deleteProject(String project) {
        checkProject(project);

        try (Handle handle = dbi.open()) {
            handle.createStatement("delete from project where name = :project")
                    .bind("project", project).execute();
        }

        super.onCreateProject(project);
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        HashMap<String, List<SchemaField>> map = new HashMap<>();
        for (TableColumn tableColumn : dao.listTableColumns(project, null)) {
            if(tableColumn.getTable().getTableName().startsWith(PrestoMaterializedViewService.MATERIALIZED_VIEW_PREFIX)) {
                continue;
            }
            TypeSignature typeSignature = tableColumn.getDataType().getTypeSignature();
            FieldType fieldType = PrestoQueryExecution.fromPrestoType(typeSignature.getBase(),
                    typeSignature.getParameters().isEmpty() ? null : typeSignature.getParameters().get(0).getTypeSignature().getBase());
            SchemaField column = new SchemaField(tableColumn.getColumnName(), fieldType);

            map.computeIfAbsent(tableColumn.getTable().getTableName(), key -> new ArrayList()).add(column);
        }
        return map;
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        return dao.listTables(project).stream()
                .map(a -> a.getTableName()).collect(Collectors.toSet());
    }

    @Override
    public void createProject(String project) {
        checkProject(project);

        try (Handle handle = reportDbi.open()) {
            handle.createStatement("INSERT INTO project (name) VALUES(:name)")
                    .bind("name", project)
                    .execute();
        }

        super.onCreateProject(project);
    }

    @Override
    public Set<String> getProjects() {
        try (Handle handle = reportDbi.open()) {
            return ImmutableSet.copyOf(
                    handle.createQuery("select name from project")
                            .map(StringMapper.FIRST).iterator());
        }
    }

    public static class SignatureReferenceType extends AbstractType {

        public SignatureReferenceType(TypeSignature signature, Class<?> javaType) {
            super(signature, javaType);
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int i, int i1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getObjectValue(ConnectorSession connectorSession, Block block, int i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(Block block, int i, BlockBuilder blockBuilder) {
            throw new UnsupportedOperationException();
        }
    }

    private static class SignatureReferenceTypeManager implements TypeManager {
        @Override
        public Type getType(TypeSignature typeSignature) {
            return new SignatureReferenceType(typeSignature, Object.class);
        }

        @Override
        public Type getParameterizedType(String s, List<TypeSignatureParameter> list) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type getParameterizedType(String s, List<TypeSignature> list, List<String> list1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Type> getCommonSuperType(List<? extends Type> list) {
            return null;
        }

        @Override
        public Optional<Type> getCommonSuperType(Type type, Type type1) {
            return null;
        }
    }

    public static String toSql(FieldType type) {
        switch (type) {
            case LONG:
                return "BIGINT";
            case STRING:
                return "VARCHAR";
            case BINARY:
                return "VARBINARY";
            case BOOLEAN:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return type.name();
            case DOUBLE:
                return "DOUBLE";
            default:
                if (type.isArray()) {
                    return "ARRAY<" + toSql(type.getArrayElementType()) + ">";
                }
                if (type.isMap()) {
                    return "MAP<VARCHAR, " + toSql(type.getMapValueType()) + ">";
                }
                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }
}
