package org.rakam.presto.analysis;

import com.facebook.presto.raptor.metadata.Distribution;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;
import org.skife.jdbi.v2.*;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.lang.invoke.MethodHandle;
import java.sql.JDBCType;
import java.time.Instant;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.*;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.type.ParameterKind.TYPE;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.sql.JDBCType.VARBINARY;
import static java.util.Locale.ENGLISH;
import static org.rakam.collection.FieldType.*;
import static org.rakam.util.ValidationUtil.checkLiteral;
import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoRakamRaptorMetastore extends PrestoAbstractMetastore {
    private final DBI dbi;
    private final MetadataDao dao;
    private final PrestoConfig prestoConfig;
    private final ProjectConfig projectConfig;

    @Inject
    public PrestoRakamRaptorMetastore(
            @Named("presto.metastore.jdbc") JDBCPoolDataSource prestoMetastoreDataSource,
            EventBus eventBus,
            ProjectConfig projectConfig,
            PrestoConfig prestoConfig) {
        super(eventBus);
        dbi = new DBI(prestoMetastoreDataSource);
        org.rakam.presto.analysis.PrestoRakamRaptorMetastore.SignatureReferenceTypeManager signatureReferenceTypeManager = new SignatureReferenceTypeManager();
        dbi.registerMapper(new TableColumn.Mapper(signatureReferenceTypeManager));
        dbi.registerMapper(new Distribution.Mapper(signatureReferenceTypeManager));
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.projectConfig = projectConfig;
        this.prestoConfig = prestoConfig;
    }

    public static <T> void daoTransaction(IDBI dbi, Class<T> daoType, Consumer<T> callback) {
        runTransaction(dbi, (handle, status) -> {
            callback.accept(handle.attach(daoType));
            return null;
        });
    }

    public static <T> T runTransaction(IDBI dbi, TransactionCallback<T> callback) {
        try {
            return dbi.inTransaction(callback);
        } catch (DBIException e) {
            propagateIfInstanceOf(e.getCause(), PrestoException.class);
            throw metadataError(e);
        }
    }

    private static String sqlColumnType(FieldType type) {
        JDBCType jdbcType = jdbcType(type);
        if (jdbcType != null) {
            switch (jdbcType) {
                case BOOLEAN:
                    return "boolean";
                case BIGINT:
                    return "bigint";
                case DOUBLE:
                    return "double";
                case INTEGER:
                    return "int";
                case VARBINARY:
                    return format("varbinary(%s)", MAX_BINARY_INDEX_SIZE);
            }
        }

        return null;
    }

    public static JDBCType jdbcType(FieldType type) {
        if (type.equals(FieldType.BOOLEAN)) {
            return JDBCType.BOOLEAN;
        }
        if (type.equals(LONG) || type.equals(TIMESTAMP)) {
            return JDBCType.BIGINT;
        }
        if (type.equals(FieldType.INTEGER)) {
            return JDBCType.INTEGER;
        }
        if (type.equals(FieldType.DOUBLE)) {
            return JDBCType.DOUBLE;
        }
        if (type.equals(FieldType.DATE)) {
            return JDBCType.INTEGER;
        }
        if (type.equals(FieldType.STRING)) {
            return VARBINARY;
        }
        return null;
    }

    public static String toSql(FieldType type) {
        switch (type) {
            case LONG:
                return StandardTypes.BIGINT;
            case STRING:
                return StandardTypes.VARCHAR;
            case BINARY:
                return StandardTypes.VARBINARY;
            case DECIMAL:
            case INTEGER:
            case BOOLEAN:
            case DATE:
            case TIME:
            case DOUBLE:
            case TIMESTAMP:
                return type.name();
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

    @PostConstruct
    @Override
    public void setup() {
        setupTables();
    }

    private void setupTables() {
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name VARCHAR(255) NOT NULL, \n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) {
        ValidationUtil.checkCollectionValid(collection);

        List<SchemaField> schemaFields = getCollection(project, collection);
        List<SchemaField> lastFields;
        Table tableInformation = dao.getTableInformation(project, collection);
        if (schemaFields.isEmpty() && tableInformation == null) {
            if (collection.startsWith("$materialized")) {
                throw new RakamException("Collections cannot start with $materialized prefix", BAD_REQUEST);
            }

            if (!getProjects().contains(project)) {
                throw new NotExistsException("Project");
            }

            List<String> params = new ArrayList<>();
            if (fields.stream().anyMatch(f -> f.getName().equals(projectConfig.getTimeColumn()) && (f.getType() == TIMESTAMP || f.getType() == DATE))) {
                params.add(format("temporal_column = '%s'", checkLiteral(projectConfig.getTimeColumn())));
            }

            dao.insertTable(project, collection, true, true, 1L, Instant.now().toEpochMilli());
            lastFields = fields.stream().collect(Collectors.toList());
        } else {
            List<SchemaField> newFields = new ArrayList<>();


            fields.stream()
                    .filter(field -> schemaFields.stream().noneMatch(f -> f.getName().equals(field.getName())))
                    .forEach(f -> {
                        if (f.getName().equals(prestoConfig.getCheckpointColumn())) {
                            throw new RakamException("Checkpoint column is reserved", BAD_REQUEST);
                        }
                        newFields.add(f);
                        try {
                            addColumn(tableInformation, project, collection, f.getName(), f.getType());
                        } catch (Exception e) {
                            if (e.getMessage().equals(METADATA_ERROR) && (e.getCause().getMessage().contains(JDBI_CONFLICT_ERROR) || e.getCause().getMessage().contains(JDBI_DUPLICATE_ERROR))) {
                                for (int i = 0; i < 50; i++) {
                                    try {
                                        addColumn(tableInformation, project, collection, f.getName(), f.getType());
                                    } catch (Exception ex) {
                                        if (ex.getMessage().equals(METADATA_ERROR) && (ex.getCause().getMessage().contains(JDBI_CONFLICT_ERROR) || ex.getCause().getMessage().contains(JDBI_DUPLICATE_ERROR))) {
                                            continue;
                                        } else if (e.getMessage().contains("exists")) {
                                            break;
                                        }

                                        throw new IllegalStateException(format("Error while adding a column called %s : %s", f.getName(), f.getType().name()), ex);
                                    }

                                    break;
                                }
                            } else if (e.getMessage().contains("exists")) {
                                // no op
                            } else {
                                throw new IllegalStateException(format("Error while adding a column called %s : %s", f.getName(), f.getType().name()), e);
                            }
                        }
                    });

            lastFields = getCollection(project, collection);
        }

        super.onCreateCollection(project, collection, schemaFields);
        return lastFields;
    }

    private static final String METADATA_ERROR = "Failed to perform metadata operation";
    private static final String JDBI_CONFLICT_ERROR = "Unique index or primary key violation";
    private static final String JDBI_DUPLICATE_ERROR = "Duplicate entry";

    private void addColumn(Table table, String schema, String tableName, String columnName, FieldType fieldType) {
        List<TableColumn> existingColumns = dao.listTableColumns(schema, tableName);
        TableColumn lastColumn = existingColumns.get(existingColumns.size() - 1);
        long columnId = lastColumn.getColumnId() + 1;
        int ordinalPosition = existingColumns.size();

        String type = TypeSignature.parseTypeSignature(toSql(fieldType)).toString().toLowerCase(ENGLISH);

        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.insertColumn(table.getTableId(), columnId, columnName, ordinalPosition, type, null, null);
            dao.updateTableVersion(table.getTableId(), System.currentTimeMillis());
        });

        String columnType = sqlColumnType(fieldType);
        if (columnType == null) {
            return;
        }

        String sql = format("ALTER TABLE %s ADD COLUMN (%s %s, %s %s)",
                shardIndexTable(table.getTableId()),
                minColumn(columnId), columnType,
                maxColumn(columnId), columnType);

        try (Handle handle = dbi.open()) {
            handle.execute(sql);
        } catch (DBIException e) {
            throw metadataError(e);
        }
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        return dao.listTableColumns(project, collection).stream()
                // this field should be removed since the server sets it
                .filter(a -> !a.getColumnName().equals(prestoConfig.getCheckpointColumn()))
                .map(column -> {
                    TypeSignature typeSignature = column.getDataType().getTypeSignature();

                    return new SchemaField(column.getColumnName(), fromPrestoType(typeSignature.getBase(),
                            typeSignature.getParameters().stream()
                                    .filter(param -> param.getKind() == TYPE)
                                    .map(param -> param.getTypeSignature().getBase()).iterator()));
                }).collect(Collectors.toList());
    }

    @Override
    public void deleteProject(String project) {
        checkProject(project);

        try (Handle handle = dbi.open()) {
            handle.createStatement("delete from project where name = :project")
                    .bind("project", project).execute();
        }

        super.onDeleteProject(project);
    }

    @Override
    public Map<String, Stats> getStats(Collection<String> projects) {
        if (projects.isEmpty()) {
            return ImmutableMap.of();
        }
        try (Handle handle = dbi.open()) {
            Map<String, Stats> map = new HashMap<>();
            for (String project : projects) {
                map.put(project, new Stats());
            }
            handle.createQuery("select schema_name, (case " +
                    "when date = CURDATE() then 'today' " +
                    "when year(date) = YEAR(NOW()) and month(date) = MONTH(NOW()) then 'month' else 'total' end) " +
                    "as date, " +
                    "sum(row_count) as events " +
                    "from (select tables.schema_name, cast(shards.create_time as date) as date, sum(shards.row_count) as row_count from tables " +
                    "join shards on (shards.table_id = tables.table_id) where schema_name in (" +
                    projects.stream().map(e -> "'" + e + "'").collect(Collectors.joining(", ")) + ") group by 1,2   ) t group by 1, 2 ").map((i, resultSet, statementContext) -> {
                Stats stats = map.get(resultSet.getString(1));
                if (resultSet.getString(2).equals("today")) {
                    stats.dailyEvents = resultSet.getLong(3);
                } else if (resultSet.getString(2).equals("month")) {
                    stats.monthlyEvents = resultSet.getLong(3);
                } else if (resultSet.getString(2).equals("total")) {
                    stats.allEvents = resultSet.getLong(3);
                }
                return null;
            }).forEach(l -> {
            });

            for (Stats stats : map.values()) {
                stats.dailyEvents = stats.dailyEvents == null ? 0 : stats.dailyEvents;
                stats.monthlyEvents = stats.monthlyEvents == null ? 0 : stats.monthlyEvents;
                stats.allEvents = stats.allEvents == null ? 0 : stats.allEvents;

                stats.allEvents += stats.monthlyEvents + stats.dailyEvents;
                stats.monthlyEvents += stats.dailyEvents;
            }

            return map;
        }
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        return getTables(project, this::filterTables);
    }

    private boolean filterTables(String tableName, String tableColumn) {
        return !tableColumn.startsWith("$")
                && !tableName.startsWith("$")
                && !tableColumn.equals(prestoConfig.getCheckpointColumn());
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        return dao.listTables(project).stream().map(e -> e.getTableName())
                .collect(Collectors.toSet());
    }

    @Override
    public void createProject(String project) {
        checkProject(project);

        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO project (name) VALUES(:name)")
                        .bind("name", project)
                        .execute();
            } catch (Exception e) {
                if (getProjects().contains(project)) {
                    throw new AlreadyExistsException("project", BAD_REQUEST);
                }
            }
        }

        super.onCreateProject(project);
    }

    @Override
    public Set<String> getProjects() {
        try (Handle handle = dbi.open()) {
            return ImmutableSet.copyOf(
                    handle.createQuery("select name from project")
                            .map(StringMapper.FIRST).iterator());
        }
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project, Predicate<String> filter) {
        return getTables(project, (t, c) -> filter.test(t));
    }

    public Map<String, List<SchemaField>> getTables(String project, BiPredicate<String, String> filter) {
        HashMap<String, List<SchemaField>> map = new HashMap<>();
        for (TableColumn tableColumn : dao.listTableColumns(project, null)) {
            if (tableColumn.getColumnName().startsWith("$") || !filter.test(tableColumn.getTable().getTableName(), tableColumn.getColumnName())) {
                continue;
            }
            TypeSignature typeSignature = tableColumn.getDataType().getTypeSignature();
            FieldType fieldType = fromPrestoType(typeSignature.getBase(),
                    typeSignature.getParameters().stream()
                            .filter(param -> param.getKind() == TYPE)
                            .map(param -> param.getTypeSignature().getBase()).iterator());
            SchemaField column = new SchemaField(tableColumn.getColumnName(), fieldType);

            map.computeIfAbsent(tableColumn.getTable().getTableName(), key -> new ArrayList()).add(column);
        }
        return map;
    }

    public static class SignatureReferenceType
            extends AbstractType {

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

    public static class SignatureReferenceTypeManager
            implements TypeManager {
        @Override
        public Type getType(TypeSignature typeSignature) {
            return new SignatureReferenceType(typeSignature, Object.class);
        }

        @Override
        public Type getParameterizedType(String s, List<TypeSignatureParameter> list) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<ParametricType> getParametricTypes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes) {
            return null;
        }

        @Override
        public Optional<Type> getCommonSuperType(Type type, Type type1) {
            return null;
        }
    }

    public static FieldType fromPrestoType(String rawType, Iterator<String> parameter) {
        switch (rawType) {
            case StandardTypes.BIGINT:
                return LONG;
            case StandardTypes.BOOLEAN:
                return BOOLEAN;
            case StandardTypes.DATE:
                return DATE;
            case StandardTypes.DOUBLE:
                return DOUBLE;
            case StandardTypes.VARBINARY:
            case StandardTypes.HYPER_LOG_LOG:
                return BINARY;
            case StandardTypes.VARCHAR:
                return STRING;
            case StandardTypes.INTEGER:
                return INTEGER;
            case StandardTypes.DECIMAL:
                return DECIMAL;
            case StandardTypes.TIME:
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return TIME;
            case StandardTypes.TIMESTAMP:
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMP;
            case StandardTypes.ARRAY:
                return fromPrestoType(parameter.next(), null).convertToArrayType();
            case StandardTypes.MAP:
                Preconditions.checkArgument(parameter.next().equals(StandardTypes.VARCHAR),
                        "The first parameter of MAP must be STRING");
                return fromPrestoType(parameter.next(), null).convertToMapValueType();
            default:
                return BINARY;
        }
    }
}
