package org.rakam.analysis.postgresql;

import com.google.common.base.Throwables;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.postgresql.util.PGobject;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.Event;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

@Singleton
public class PostgresqlEventStore implements EventStore {
    private final Set<String> sourceFields;
    private final JDBCPoolDataSource connectionPool;

    @Inject
    public PostgresqlEventStore(@Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, FieldDependencyBuilder.FieldDependency fieldDependency) {
        this.connectionPool = connectionPool;
        this.sourceFields = fieldDependency.dependentFields.keySet();
    }

    @Override
    public void store(org.rakam.collection.Event event) {
        GenericRecord record = event.properties();
        try(Connection connection = connectionPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(getQuery(event));
            bindParam(connection, ps, event.properties().getSchema().getFields(), record);
            ps.executeUpdate();
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void storeBatch(List<Event> events) {
        try(Connection connection = connectionPool.getConnection()) {
            connection.setAutoCommit(false);
            for (Event event : events) {
                GenericRecord record = event.properties();
                PreparedStatement ps = connection.prepareStatement(getQuery(event));
                bindParam(connection, ps, event.properties().getSchema().getFields(), record);

                ps.executeUpdate();
            }
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
    }

    private Schema getActualType(Schema.Field field) {
        Schema schema = field.schema();
        if(schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().get(1);
        } else {
            return schema;
        }
    }

    private void bindParam(Connection connection, PreparedStatement ps, List<Schema.Field> fields, GenericRecord record) throws SQLException {
        Object value;
        int pos = 1;
        for (Schema.Field field : fields) {
            value = record.get(field.pos());

            if(sourceFields.contains(field.name())) {
                continue;
            }

            if(value == null) {
                ps.setNull(pos++, 0);
                continue;
            }

            Schema schema = getActualType(field);
            switch (schema.getType()) {
                case ARRAY:
                    String typeName = toPostgresqlPrimitiveTypeName(schema.getElementType().getType());
                    ps.setArray(pos++, connection.createArrayOf(typeName, ((List) value).toArray()));
                    break;
                case MAP:
                    PGobject jsonObject = new PGobject();
                    jsonObject.setType("jsonb");
                    jsonObject.setValue(JsonHelper.encode(value));
                    ps.setObject(pos++, jsonObject);
                    break;
                case STRING:
                    ps.setString(pos++, (String) value);
                    break;
                case INT:
                    ps.setInt(pos++, ((Number) value).intValue());
                    break;
                case LONG:
                    ps.setLong(pos++, ((Number) value).longValue());
                    break;
                case FLOAT:
                    ps.setFloat(pos++, ((Number) value).floatValue());
                    break;
                case DOUBLE:
                    ps.setDouble(pos++, ((Number) value).doubleValue());
                    break;
                case BOOLEAN:
                    ps.setBoolean(pos++, (Boolean) value);
                    break;
            }
        }
    }

    public static String toPostgresqlPrimitiveTypeName(Schema.Type fieldType) {
        switch (fieldType) {
            case STRING:
                return "text";
            case LONG:
                return "bigint";
            case DOUBLE:
                return "double precision";
            case BOOLEAN:
                return "boolean";
            default:
                return "bigint";
        }
    }

    private String getQuery(Event event) {
        // since we don't cache queries, we should care about performance so we just use StringBuilder instead of streams.
        // String columns = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.joining(", "));
        // String parameters = schema.getFields().stream().map(f -> "?").collect(Collectors.joining(", "));
        StringBuilder query = new StringBuilder("INSERT INTO ")
                .append(event.project())
                .append(".")
                .append(event.collection());
        StringBuilder params = new StringBuilder();
        Schema schema = event.properties().getSchema();
        List<Schema.Field> fields = schema.getFields();

        Schema.Field f = fields.get(0);
        if(!sourceFields.contains(f.name())) {
            query.append(" (\"").append(f.name());
            params.append("?");
        }

        for (int i = 1; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);

            if(!sourceFields.contains(field.name())) {
                query.append("\", \"").append(field.name());
                params.append(", ?");
            }
        }

        return query.append("\") VALUES (").append(params.toString()).append(")").toString();
    }
}
