package org.rakam.postgresql.analysis;

import com.google.common.collect.ImmutableList;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.postgresql.util.PGobject;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.util.JsonHelper;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.rakam.postgresql.PostgresqlModule.PostgresqlVersion.Version.PG10;
import static org.rakam.util.ValidationUtil.*;

@Singleton
public class PostgresqlEventStore
        implements SyncEventStore {
    public static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")));
    private final static Logger LOGGER = Logger.get(PostgresqlEventStore.class);
    private final Set<String> sourceFields;
    private final JDBCPoolDataSource connectionPool;
    private final PostgresqlModule.PostgresqlVersion version;

    @Inject
    public PostgresqlEventStore(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, PostgresqlModule.PostgresqlVersion version, FieldDependency fieldDependency) {
        this.connectionPool = connectionPool;
        this.version = version;
        this.sourceFields = fieldDependency.dependentFields.keySet();
    }

    public static String toPostgresqlPrimitiveTypeName(FieldType type) {
        switch (type) {
            case LONG:
                return "int8";
            case INTEGER:
                return "int4";
            case DECIMAL:
                return "decimal";
            case STRING:
                return "text";
            case BOOLEAN:
                return "bool";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case TIMESTAMP:
                return "timestamp";
            case DOUBLE:
                return "float8";
            default:
                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }

    @Override
    public void store(Event event) {
        store(event, false);
    }

    public void store(Event event, boolean partitionCheckDone) {
        GenericRecord record = event.properties();
        try (Connection connection = connectionPool.getConnection()) {
            Schema schema = event.properties().getSchema();
            PreparedStatement ps = connection.prepareStatement(getQuery(event.project(), event.collection(), schema));
            bindParam(connection, ps, event.schema(), record);
            ps.executeUpdate();
        } catch (SQLException e) {
            // check_violation -> https://www.postgresql.org/docs/8.2/static/errcodes-appendix.html
            if (version.getVersion() == PG10 && !partitionCheckDone && "23514".equals(e.getSQLState())) {
                generateMissingPartitions(event.project(), event.collection(), ImmutableList.of(event), 0);
                store(event, true);
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void generateMissingPartitions(String project, String collection, List<Event> events, int startFrom) {

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")));

        HashSet<String> set = new HashSet<>();
        for (int i = startFrom; i < events.size(); i++) {
            Event event = events.get(i);
            Number time = event.getAttribute("_time");
            cal.setTimeInMillis(time.longValue());
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONDAY) + 1;
            set.add(year + "_" + month);
        }

        try (Connection connection = connectionPool.getConnection()) {

            HashSet<String> missingPartitions;
            Statement statement = connection.createStatement();

            if (set.size() > 1) {
                missingPartitions = new HashSet<>();

                String values = set.stream().map(item -> format("('%s')", item)).collect(Collectors.joining(", "));
                ResultSet resultSet = statement.executeQuery(format("select part from (VALUES%s) data (part) where part not in (\n" +
                        "\tselect substring(cchild.relname, %d)  from pg_catalog.pg_class c \n" +
                        "\tJOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \n" +
                        "\tJOIN pg_inherits i on (i.inhparent = c.relfilenode)\n" +
                        "\tJOIN pg_catalog.pg_class cchild ON cchild.relfilenode = i.inhrelid \n" +
                        "\tWHERE n.nspname = '%s' and c.relname = '%s'\n" +
                        ")", values, collection.length() + 2, project, collection));
                while (resultSet.next()) {
                    missingPartitions.add(resultSet.getString(1));
                }
            } else {
                missingPartitions = set;
            }

            for (String missingPartition : missingPartitions) {
                String[] split = missingPartition.split("_", 2);
                int year = Integer.parseInt(split[0]);
                int month = Integer.parseInt(split[1]);

                try {
                    statement.execute(format("CREATE TABLE %s.\"%s~%d_%d\" PARTITION OF %s.%s\n" +
                                    "FOR VALUES FROM ('%s-%d-1 00:00:00.000000') to ( '%s-%d-1 00:00:00.000000')",
                            checkProject(project, '"'),
                            collection.replaceAll("\"", ""),
                            year, month,
                            checkProject(project, '"'),
                            checkCollection(collection, '"'),
                            year, month,
                            month == 12 ? year + 1 : year,
                            month == 12 ? 1 : month + 1));
                } catch (SQLException e) {
                    if (!"42P07".equals(e.getSQLState())) {
                        throw new RuntimeException(e);
                    }
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int storeBatchInline(Connection connection, String collection, List<Event> eventsForCollection, int checkpoint, boolean partitionCheckDone) throws SQLException {

        // last event must have the last schema
        Event lastEvent = getLastEvent(eventsForCollection);

        PreparedStatement ps = connection.prepareStatement(getQuery(lastEvent.project(),
                collection, lastEvent.properties().getSchema()));

        int lastCheckpoint = checkpoint;
        for (int i = checkpoint; i < eventsForCollection.size(); i++) {
            Event event = eventsForCollection.get(i);
            GenericRecord properties = event.properties();
            bindParam(connection, ps, lastEvent.schema(), properties);
            ps.addBatch();
            if (i > 0 && i % 5000 == 0) {
                try {
                    ps.executeBatch();
                } catch (SQLException e) {
                    // check_violation -> https://www.postgresql.org/docs/8.2/static/errcodes-appendix.html
                    if (version.getVersion() == PG10 && !partitionCheckDone && "23514".equals(e.getSQLState())) {
                        generateMissingPartitions(event.project(), collection, eventsForCollection, 0);
                        ps.cancel();
                        connection.rollback();
                        storeBatchInline(connection, collection, eventsForCollection, lastCheckpoint, true);
                    } else {
                        return lastCheckpoint;
                    }
                }

                lastCheckpoint = i;
            }
        }

        try {
            ps.executeBatch();
        } catch (SQLException e) {
            // check_violation -> https://www.postgresql.org/docs/8.2/static/errcodes-appendix.html
            if (version.getVersion() == PG10 && !partitionCheckDone && "23514".equals(e.getSQLState())) {
                generateMissingPartitions(lastEvent.project(), collection, eventsForCollection, 0);
                ps.cancel();
                connection.rollback();
                storeBatchInline(connection, collection, eventsForCollection, lastCheckpoint, true);
            } else {
                return lastCheckpoint;
            }
        }

        connection.commit();
        return eventsForCollection.size();
    }

    @Override
    public int[] storeBatch(List<Event> events) {
        Map<String, List<Event>> groupedByCollection = events.stream()
                .collect(Collectors.groupingBy(Event::collection));

        Map<String, Integer> successfulCollections = new HashMap<>(groupedByCollection.size());
        try (Connection connection = connectionPool.getConnection()) {
            connection.setAutoCommit(false);

            for (Map.Entry<String, List<Event>> entry : groupedByCollection.entrySet()) {
                int result = storeBatchInline(connection, entry.getKey(), entry.getValue(), 0, false);
                successfulCollections.put(entry.getKey(), result);
            }

            connection.setAutoCommit(true);
            List<Integer> result = null;
            for (Map.Entry<String, List<Event>> entries : groupedByCollection.entrySet()) {
                int numberOfEvents = entries.getValue().size();
                int checkPoint = Optional.of(successfulCollections.get(entries.getKey())).orElse(0);
                if (numberOfEvents > checkPoint) {
                    if (result == null) {
                        result = new ArrayList<>();
                    }
                    for (int checkpoint = 0; checkpoint < numberOfEvents; checkpoint++) {
                        result.add(events.indexOf(entries.getValue().get(checkPoint)));
                    }
                }
            }

            if (result == null) {
                return EventStore.SUCCESSFUL_BATCH;
            } else {
                return result.stream().mapToInt(i -> i).toArray();
            }
        } catch (SQLException e) {
            List<Event> sample = events.size() > 5 ? events.subList(0, 5) : events;

            LOGGER.error(e.getNextException() != null ? e.getNextException() : e,
                    "Error while storing events in Postgresql batch query: " + sample);

            return IntStream.range(0, events.size()).filter(idx -> {
                Event event = events.get(idx);
                Integer checkpointPosition = successfulCollections.get(event.collection());
                return checkpointPosition == null ||
                        groupedByCollection.get(event.collection()).indexOf(event) > checkpointPosition;
            }).toArray();
        }
    }

    // get the event with the last schema
    private Event getLastEvent(List<Event> eventsForCollection) {
        Event event = eventsForCollection.get(0);
        for (int i = 1; i < eventsForCollection.size(); i++) {
            Event newEvent = eventsForCollection.get(i);
            if (newEvent.schema().size() > event.schema().size()) {
                event = newEvent;
            }
        }
        return event;
    }

    private void bindParam(Connection connection, PreparedStatement ps, List<SchemaField> fields, GenericRecord record)
            throws SQLException {
        Object value;
        for (int i = 0; i < fields.size(); i++) {
            SchemaField field = fields.get(i);
            value = record.get(field.getName());

            if (value == null) {
                ps.setNull(i + 1, 0);
                continue;
            }

            FieldType type = field.getType();
            switch (type) {
                case STRING:
                    String val = (String) value;
                    if (val != null && val.length() > 100) {
                        val = val.substring(0, 100);
                    }
                    ps.setString(i + 1, val);
                    break;
                case LONG:
                    ps.setLong(i + 1, ((Number) value).longValue());
                    break;
                case INTEGER:
                    ps.setInt(i + 1, ((Number) value).intValue());
                    break;
                case DECIMAL:
                    ps.setBigDecimal(i + 1, new BigDecimal(((Number) value).doubleValue()));
                    break;
                case DOUBLE:
                    ps.setDouble(i + 1, ((Number) value).doubleValue());
                    break;
                case TIMESTAMP:
                    Timestamp x = new Timestamp(((Number) value).longValue());
                    if (x.getTime() < 0) {
                        ps.setTimestamp(i + 1, null);
                    } else {
                        ps.setTimestamp(i + 1, x, UTC_CALENDAR);
                    }
                    break;
                case TIME:
                    ps.setTime(i + 1, Time.valueOf(LocalTime.ofSecondOfDay(((Number) value).intValue())), UTC_CALENDAR);
                    break;
                case DATE:
                    ps.setDate(i + 1, Date.valueOf(LocalDate.ofEpochDay(((Number) value).intValue())));
                    break;
                case BOOLEAN:
                    ps.setBoolean(i + 1, (Boolean) value);
                    break;
                case BINARY:
                    ps.setBytes(i + 1, (byte[]) value);
                    break;
                default:
                    if (type.isArray()) {
                        String typeName = toPostgresqlPrimitiveTypeName(type.getArrayElementType());
                        ps.setArray(i + 1, connection.createArrayOf(typeName, ((List) value).toArray()));
                    } else if (type.isMap()) {
                        PGobject jsonObject = new PGobject();
                        jsonObject.setType("jsonb");
                        jsonObject.setValue(JsonHelper.encode(value));
                        ps.setObject(i + 1, jsonObject);
                    } else {
                        throw new UnsupportedOperationException();
                    }
            }
        }
    }

    private String getQuery(String project, String collection, Schema schema) {
        StringBuilder query = new StringBuilder("INSERT INTO ")
                .append(checkProject(project, '"'))
                .append(".")
                .append(ValidationUtil.checkCollection(collection));
        StringBuilder params = new StringBuilder();
        List<Schema.Field> fields = schema.getFields();

        Schema.Field firstField = fields.get(0);
        if (!sourceFields.contains(firstField.name())) {
            query.append(" (").append(checkTableColumn(firstField.name()));
            params.append('?');
        }

        for (int i = 1; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);

            if (!sourceFields.contains(field.name())) {
                query.append(", ").append(checkTableColumn(field.name()));
                params.append(", ?");
            }
        }

        return query.append(") VALUES (").append(params.toString()).append(")").toString();
    }
}
