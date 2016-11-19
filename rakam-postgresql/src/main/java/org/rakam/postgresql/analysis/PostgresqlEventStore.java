package org.rakam.postgresql.analysis;

import com.google.common.base.Throwables;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.postgresql.util.PGobject;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;
import org.rakam.util.JsonHelper;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.rakam.util.ValidationUtil.checkTableColumn;

@Singleton
public class PostgresqlEventStore
        implements SyncEventStore
{
    private final static Logger LOGGER = Logger.get(PostgresqlEventStore.class);

    private final Set<String> sourceFields;
    private final JDBCPoolDataSource connectionPool;
    public static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")));

    @Inject
    public PostgresqlEventStore(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, FieldDependency fieldDependency)
    {
        this.connectionPool = connectionPool;
        this.sourceFields = fieldDependency.dependentFields.keySet();
    }

    @Override
    public void store(Event event)
    {
        GenericRecord record = event.properties();
        try (Connection connection = connectionPool.getConnection()) {
            Schema schema = event.properties().getSchema();
            PreparedStatement ps = connection.prepareStatement(getQuery(event.project(), event.collection(), schema));
            bindParam(connection, ps, event.schema(), record);
            ps.executeUpdate();
        }
        catch (SQLException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public int[] storeBatch(List<Event> events)
    {
        Map<String, List<Event>> groupedByCollection = events.stream()
                .collect(Collectors.groupingBy(Event::collection));

        Map<String, Integer> successfulCollections = new HashMap<>(groupedByCollection.size());
        try (Connection connection = connectionPool.getConnection()) {
            for (Map.Entry<String, List<Event>> entry : groupedByCollection.entrySet()) {
                connection.setAutoCommit(false);
                // last event must have the last schema
                List<Event> eventsForCollection = entry.getValue();
                Event lastEvent = getLastEvent(eventsForCollection);

                PreparedStatement ps = connection.prepareStatement(getQuery(lastEvent.project(),
                        entry.getKey(), lastEvent.properties().getSchema()));

                for (int i = 0; i < eventsForCollection.size(); i++) {
                    Event event = eventsForCollection.get(i);
                    GenericRecord properties = event.properties();
                    bindParam(connection, ps, event.schema(), properties);
                    ps.addBatch();
                    if (i > 0 && i % 5000 == 0) {
                        ps.executeBatch();

                        Integer value = successfulCollections.get(entry.getKey());
                        if(value == null) {
                            successfulCollections.put(entry.getKey(), i);
                        } else {
                            successfulCollections.put(entry.getKey(), i + value);
                        }
                    }
                }

                ps.executeBatch();

                connection.commit();
                successfulCollections.compute(entry.getKey(), (k, v) -> eventsForCollection.size());
            }

            connection.setAutoCommit(true);
            return EventStore.SUCCESSFUL_BATCH;
        }
        catch (SQLException e) {
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
    private Event getLastEvent(List<Event> eventsForCollection)
    {
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
            throws SQLException
    {
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
                    ps.setString(i + 1, (String) value);
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
                    }
                    else {
                        ps.setTimestamp(i + 1, x, UTC_CALENDAR);
                    }
                    break;
                case TIME:
                    ps.setTime(i + 1, Time.valueOf(LocalTime.ofSecondOfDay(((Number) value).intValue())), UTC_CALENDAR);
                    break;
                case DATE:
                    ps.setDate(i + 1, Date.valueOf(LocalDate.ofEpochDay(((Number) value).intValue())), UTC_CALENDAR);
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
                    }
                    else if (type.isMap()) {
                        PGobject jsonObject = new PGobject();
                        jsonObject.setType("jsonb");
                        jsonObject.setValue(JsonHelper.encode(value));
                        ps.setObject(i + 1, jsonObject);
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
            }
        }
    }

    private String getQuery(String project, String collection, Schema schema)
    {
        StringBuilder query = new StringBuilder("INSERT INTO ")
                .append(project)
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

    public static String toPostgresqlPrimitiveTypeName(FieldType type)
    {
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
}
