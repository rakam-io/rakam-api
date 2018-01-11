package org.rakam.clickhouse.analysis;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import java.time.LocalDate;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static org.rakam.clickhouse.analysis.ClickHouseQueryExecution.parseClickhouseType;
import static org.rakam.collection.FieldType.TIMESTAMP;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class ClickHouseMetastore
        extends AbstractMetastore {
    private final ClickHouseConfig config;
    private final ProjectConfig projectConfig;

    @Inject
    public ClickHouseMetastore(ClickHouseConfig config, ProjectConfig projectConfig, EventBus eventBus) {
        super(eventBus);
        this.config = config;
        this.projectConfig = projectConfig;
    }

    public static String toClickHouseType(FieldType type) {
        switch (type) {
            case INTEGER:
            case TIME:
                return "Int32";
            case LONG:
                return "Int64";
            case STRING:
            case BINARY:
                return "String";
            case BOOLEAN:
                return "UInt8";
            case DATE:
                return "Date";
            case TIMESTAMP:
                return "DateTime";
            case DECIMAL:
            case DOUBLE:
                return "Float64";
            default:
                if (type.isArray()) {
                    return "Array(" + toClickHouseType(type.getArrayElementType()) + ")";
                }
                if (type.isMap()) {
                    return "Nested(Key String, Value " + toClickHouseType(type.getMapValueType()) + ")";
                }

                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        List<List<Object>> data = new ClickHouseQueryExecution(config, format("select table, name, type from system.columns where database = '%s'",
                project)).getResult().join().getResult();

        HashMap<String, List<SchemaField>> map = new HashMap<>();
        data.stream().forEach(list -> {
            SchemaField schemaField = new SchemaField(list.get(1).toString(),
                    parseClickhouseType(list.get(2).toString()));
            map.computeIfAbsent(list.get(0).toString(), (k) -> new ArrayList<>())
                    .add(schemaField);
        });

        return map;
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        List<List<Object>> data = new ClickHouseQueryExecution(config, format("select name from system.columns where database = '%s' and name not like '$%%'",
                project)).getResult().join().getResult();
        return data.stream().map(e -> e.get(0).toString()).collect(Collectors.toSet());
    }

    @Override
    public void createProject(String project) {
        StringResponse resp = ClickHouseQueryExecution.runStatementSafe(config, format("CREATE DATABASE `%s`", project));
        if (resp.getStatusCode() != 200) {
            if (resp.getBody().startsWith("Code: 82")) {
                throw new AlreadyExistsException("Project", BAD_REQUEST);
            }
            throw new RakamException(resp.getBody().split("\n", 2)[0], INTERNAL_SERVER_ERROR);
        }

        super.onCreateProject(project);
    }

    @Override
    public Set<String> getProjects() {
        List<List<Object>> databases = new ClickHouseQueryExecution(config, "SHOW DATABASES").getResult().join().getResult();
        return databases.stream().map(e -> e.get(0).toString()).collect(Collectors.toSet());
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        List<List<Object>> data = new ClickHouseQueryExecution(config, format("select name, type from system.columns where database = '%s' and table = '%s' and name not like '$%%'",
                project, collection)).getResult().join().getResult();

        return data.stream().map(list -> new SchemaField(list.get(0).toString(),
                parseClickhouseType(list.get(1).toString())))
                .collect(Collectors.toList());
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields)
            throws NotExistsException {
        ValidationUtil.checkCollectionValid(collection);
        return getOrCreateCollectionFields(project, collection, fields, fields.size());
    }

    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields, final int tryCount)
            throws NotExistsException {
        String query;
        List<SchemaField> schemaFields = getCollection(project, collection);
        List<SchemaField> lastFields;
        if (schemaFields.isEmpty()) {
            List<SchemaField> currentFields = new ArrayList<>();

            if (!getProjects().contains(project)) {
                throw new NotExistsException("Project");
            }
            String queryEnd = fields.stream()
                    .map(f -> {
                        currentFields.add(f);
                        return f;
                    })
                    .map(f -> format("%s %s", checkTableColumn(f.getName(), '`'), toClickHouseType(f.getType())))
                    .collect(Collectors.joining(", "));
            if (queryEnd.isEmpty()) {
                return currentFields;
            }
            boolean timeActive = fields.stream().anyMatch(f -> f.getName().equals(projectConfig.getTimeColumn()) && f.getType() == TIMESTAMP);
            if (!timeActive) {
                throw new RakamException("ClickHouse requires time property", BAD_REQUEST);
            }

            Optional<SchemaField> userColumn = fields.stream().filter(f -> f.getName().equals("_user")).findAny();

            String properties;
            if (userColumn.isPresent()) {
                String hashFunction = userColumn.get().getType().isNumeric() ? "intHash32" : "cityHash64";
                properties = format("ENGINE = MergeTree(`$date`, %s(_user), (`$date`, %s(_user)), 8192)", hashFunction, hashFunction);
            } else {
                properties = "ENGINE = MergeTree(`$date`, (`$date`), 8192)";
            }

            query = format("CREATE TABLE %s.%s (`$date` Date, %s) %s ",
                    project, checkCollection(collection, '`'), queryEnd, properties);

            StringResponse join = ClickHouseQueryExecution.runStatementSafe(config, query);

            if (join.getStatusCode() != 200) {
                if (join.getBody().startsWith("Code: 44") || join.getBody().startsWith("Code: 57")) {
                    if (tryCount > 0) {
                        return getOrCreateCollectionFields(project, collection, fields, tryCount - 1);
                    } else {
                        throw new RakamException(String.format("Failed to add new fields to collection %s.%s: %s",
                                project, collection, Arrays.toString(fields.toArray())),
                                INTERNAL_SERVER_ERROR);
                    }
                } else {
                    throw new IllegalStateException(join.getBody());
                }
            }

            lastFields = fields.stream().collect(Collectors.toList());
        } else {
            List<SchemaField> newFields = new ArrayList<>();

            fields.stream()
                    .filter(field -> schemaFields.stream().noneMatch(f -> f.getName().equals(field.getName())))
                    .forEach(f -> {
                        newFields.add(f);
                        String q = format("ALTER TABLE %s.%s ADD COLUMN `%s` %s",
                                project, checkCollection(collection, '`'),
                                f.getName(), toClickHouseType(f.getType()));

                        StringResponse join = ClickHouseQueryExecution.runStatementSafe(config, q);
                        if (join.getStatusCode() != 200) {
                            if (!getCollection(project, collection).stream().anyMatch(e -> e.getName().equals(f.getName()))) {
                                throw new IllegalStateException(join.getBody());
                            }
                        }
                    });

            lastFields = getCollection(project, collection);
        }

        super.onCreateCollection(project, collection, schemaFields);
        return lastFields;
    }

    @Override
    public void deleteProject(String project) {

    }

    @Override
    public CompletableFuture<List<String>> getAttributes(String project, String collection, String attribute, Optional<LocalDate> startDate,
                                                         Optional<LocalDate> endDate, Optional<String> filter) {
        throw new UnsupportedOperationException();
    }
}
