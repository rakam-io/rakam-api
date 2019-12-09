package org.rakam.presto.analysis;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import io.airlift.log.Logger;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.presto.analysis.MetadataDao.TableColumn;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.ValidationUtil;
import org.skife.jdbi.v2.*;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.lang.invoke.MethodHandle;
import java.sql.JDBCType;
import java.time.Instant;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.type.ParameterKind.TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.sql.JDBCType.VARBINARY;
import static java.util.Locale.ENGLISH;
import static org.rakam.collection.FieldType.*;
import static org.rakam.util.ValidationUtil.checkProject;

public class MysqlExplicitMetastore extends AbstractMetastore {
    private final static Logger LOGGER = Logger.get(MysqlExplicitMetastore.class);

    private final DBI dbi;
    private final MetadataDao dao;

    @Inject
    public MysqlExplicitMetastore(
            @Named("metadata.store.jdbc") JDBCPoolDataSource prestoMetastoreDataSource,
            EventBus eventBus) {
        super(eventBus);
        dbi = new DBI(prestoMetastoreDataSource);
        MysqlExplicitMetastore.SignatureReferenceTypeManager signatureReferenceTypeManager = new SignatureReferenceTypeManager();
        dbi.registerMapper(new TableColumn.Mapper(signatureReferenceTypeManager));
        this.dao = onDemandDao(dbi, MetadataDao.class);
    }

    @PostConstruct
    public void installSchema() {
        SchemaDao schemaDao = dbi.onDemand(SchemaDao.class);
        schemaDao.createTable();
        schemaDao.createColumn();
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

    @PostConstruct
    @Override
    public void setup() {
        setupTables();
    }

    private void setupTables() {
        dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS project (" +
                    "  name VARCHAR(255) NOT NULL, \n" +
                    "  is_active BOOLEAN NOT NULL DEFAULT TRUE, \n" +
                    "  PRIMARY KEY (name))")
                    .execute();
            return null;
        });
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) {
        ValidationUtil.checkCollectionValid(collection);

        List<SchemaField> schemaFields = getCollection(project, collection);
        if (schemaFields.isEmpty()) {
            // the table does not exist
            try {
                dao.insertTable(project, collection, 1, Instant.now().toEpochMilli());
            } catch (PrestoException e) {
                if(!(e.getCause().getCause() instanceof MySQLIntegrityConstraintViolationException)) {
                    // the table is already created
                    throw e;
                }
            }
        }

        List<SchemaField> newFields = fields.stream()
                .filter(field -> schemaFields.stream().noneMatch(f -> f.getName().equals(field.getName())))
                .collect(Collectors.toList());

        if(!newFields.isEmpty()) {
            newFields.forEach(f -> addColumn(project, collection, f.getName(), f.getType()));
            super.onCreateCollection(project, collection, schemaFields);
            return getCollection(project, collection);
        } else {
            return schemaFields;
        }
    }

    private void addColumn(String project, String collection, String columnName, FieldType fieldType) {
        String type = TypeSignature.parseTypeSignature(toSql(fieldType)).toString().toLowerCase(ENGLISH);

        try {
            dao.insertColumn(project, collection, columnName, type, System.currentTimeMillis());
        } catch (PrestoException e) {
            if(!(e.getCause().getCause() instanceof MySQLIntegrityConstraintViolationException)) {
                // the table is already created
                throw e;
            }
        }
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        return dao.listTableColumns(project, collection).stream()
                // this field should be removed since the server sets it
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
    public Map<String, List<SchemaField>> getCollections(String project) {
        return getTables(project, this::filterTables);
    }

    private boolean filterTables(String tableName, String tableColumn) {
        return !tableColumn.startsWith("$") && !tableName.startsWith("$");
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        return dao.listTables(project).stream().map(e -> e.getTableName()).collect(Collectors.toSet());
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
}
