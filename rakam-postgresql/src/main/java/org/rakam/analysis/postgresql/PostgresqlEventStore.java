package org.rakam.analysis.postgresql;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.rakam.PostgresqlPoolDataSource;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 16:22.
 */
@Singleton
public class PostgresqlEventStore implements EventStore {
    PostgresqlPoolDataSource connectionPool;

    @Inject
    public PostgresqlEventStore(PostgresqlPoolDataSource connectionPool) {
        this.connectionPool = connectionPool;
    }

    @Override
    public void store(org.rakam.collection.Event event) {
        GenericRecord record = event.properties();
        try(Connection connection = connectionPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(getQuery(event));
            for (Schema.Field field : event.properties().getSchema().getFields()) {
                bindParam(connection, ps, field, record.get(field.pos()));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
    }

    private void bindParam(Connection connection, PreparedStatement ps, Schema.Field field, Object value) throws SQLException {
        Schema.Type type = field.schema().getType();
        if(type == Schema.Type.UNION) {
            type = field.schema().getTypes().get(1).getType();
        }
        int pos = field.pos()+1;
        switch (type) {
            case ARRAY:
                ps.setArray(pos, connection.createArrayOf("varchar", ((List) value).toArray()));
                break;
            case STRING:
                ps.setString(pos, (String) value);
                break;
            case INT:
                ps.setInt(pos, ((Number) value).intValue());
                break;
            case LONG:
                ps.setLong(pos, ((Number) value).longValue());
                break;
            case FLOAT:
                ps.setFloat(pos, ((Number) value).floatValue());
                break;
            case DOUBLE:
                ps.setDouble(pos, ((Number) value).doubleValue());
                break;
            case BOOLEAN:
                ps.setBoolean(pos, (Boolean) value);
                break;
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

        query.append(" (\"").append(fields.get(0).name());
        params.append("?");

        for (int i = 1; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            query.append("\", \"").append(field.name());
            params.append(", ?");
        }

        return query.append("\") VALUES (").append(params.toString()).append(")").toString();
    }

//    private PooledObjectFactory initializePool(Schema schema) {
//        return new PooledObjectFactory<Tuple<Connection, PreparedStatement>>() {
//            @Override
//            public PooledObject<Tuple<Connection, PreparedStatement>> makeObject() throws Exception {
//
//
//                Connection connection = DriverManager.getConnection(
//                        format("jdbc:postgresql://%s/%s", postgresqlConfig.getHost(), postgresqlConfig.getDatabase()),
//                        postgresqlConfig.getUsername(),
//                        postgresqlConfig.getPassword());
//                PreparedStatement statement = connection.prepareStatement("INSERT INTO %s (%s) VALUES (%s)", postgresqlConfig.getT);
//                return new DefaultPooledObject<>(new Tuple<>(connection, statement));
//            }
//
//            @Override
//            public void destroyObject(PooledObject<Tuple<Connection, PreparedStatement>> p) throws Exception {
//                Tuple<Connection, PreparedStatement> object = p.getObject();
//                object.v2().close();
//                object.v1().close();
//            }
//
//            @Override
//            public boolean validateObject(PooledObject<Tuple<Connection, PreparedStatement>> p) {
//                try {
//                    return !p.getObject().v1().isClosed();
//                } catch (SQLException e) {
//                    return false;
//                }
//            }
//
//            @Override
//            public void activateObject(PooledObject<Tuple<Connection, PreparedStatement>> p) throws Exception {
//
//            }
//
//            @Override
//            public void passivateObject(PooledObject<Tuple<Connection, PreparedStatement>> p) throws Exception {
//                p.getObject().v2().clearParameters();
//            }
//        };
//    }
}
