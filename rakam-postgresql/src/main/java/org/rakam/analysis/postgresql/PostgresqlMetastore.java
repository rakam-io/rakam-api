package org.rakam.analysis.postgresql;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.rakam.PostgresqlPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.Column;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkProject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 19:09.
 */
public class PostgresqlMetastore implements Metastore {
    PostgresqlPoolDataSource connectionPool;

    @Inject
    public PostgresqlMetastore(PostgresqlPoolDataSource connectionPool) {
        this.connectionPool = connectionPool;

        try(Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("" +
                    "  CREATE TABLE IF NOT EXISTS public.collections_last_sync (" +
                    "  project TEXT NOT NULL," +
                    "  collection TEXT NOT NULL," +
                    "  last_sync int4 NOT NULL," +
                    "  PRIMARY KEY (project, collection)" +
                    "  )");
        } catch (SQLException e) {
           Throwables.propagate(e);
        }
    }

    @Override
    public Map<String, List<String>> getAllCollections() {
        Map<String, List<String>> map = Maps.newHashMap();
        try(Connection connection = connectionPool.getConnection()) {
            ResultSet dbColumns = connection.getMetaData().getTables("", null, null, null);
            while (dbColumns.next()) {
                String schemaName = dbColumns.getString("TABLE_SCHEM");
                if(schemaName.equals("information_schema") || schemaName.startsWith("pg_")) {
                    continue;
                }
                String tableName = dbColumns.getString("TABLE_NAME");
                List<String> table = map.get(schemaName);
                if(table == null) {
                    table = Lists.newLinkedList();
                    map.put(schemaName, table);
                }
                table.add(tableName);
            }
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
        return map;
    }

//    @Override
//    public Map<String, List<SchemaField>> getCollections(String project) {
//        checkProject(project);
//        Map<String, List<SchemaField>> table = Maps.newHashMap();
//
//        try(Connection connection = connectionPool.getConnection()) {
//            ResultSet resultSet = connection.createStatement().executeQuery("select column_name, data_type, table_name, is_nullable\n" +
//                    "from INFORMATION_SCHEMA.COLUMNS where table_schema = '"+project+"' and table_name not like '\\_%' ESCAPE '\\'");
//            while (resultSet.next()) {
//                String tableName = resultSet.getString("table_name");
//                List<SchemaField> schemaFields = table.get(tableName);
//                if(schemaFields == null) {
//                    schemaFields = new LinkedList<>();
//                    table.put(tableName, schemaFields);
//                }
//                schemaFields.add(new SchemaField(
//                        resultSet.getString("column_name"),
//                        fromSql(resultSet.getString("data_type")),
//                        resultSet.getString("is_nullable").equals("YES")));
//            }
//        } catch (SQLException e) {
//            Throwables.propagate(e);
//        }
//        return table;
//    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        checkProject(project);
        Map<String, List<SchemaField>> table = Maps.newHashMap();

        try(Connection connection = connectionPool.getConnection()) {
            HashSet<String> tables = new HashSet<>();
            ResultSet tableRs = connection.getMetaData().getTables("", project, null, new String[]{"TABLE"});
            while(tableRs.next()) {
                String tableName = tableRs.getString("table_name");

                if(!tableName.startsWith("_")) {
                    tables.add(tableName);
                }
            }
            ResultSet resultSet = connection.getMetaData().getColumns("", project, null, null);
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                // TODO: move it to tableNamePattern parameter in DatabaseMetadata.getColumns()
                if(!tables.contains(tableName)) {
                    continue;
                }
                List<SchemaField> schemaFields = table.get(tableName);
                if(schemaFields == null) {
                    schemaFields = new LinkedList<>();
                    table.put(tableName, schemaFields);
                }
                schemaFields.add(new SchemaField(
                        resultSet.getString("COLUMN_NAME"),
                        fromSql(resultSet.getInt("DATA_TYPE")),
                        resultSet.getString("NULLABLE").equals("1")));
            }
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
        return table;
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        List<SchemaField> schemaFields = Lists.newArrayList();
        try(Connection connection = connectionPool.getConnection()) {
            ResultSet dbColumns = connection.getMetaData().getColumns("", project, collection, null);
            while (dbColumns.next()) {
                String columnName = dbColumns.getString("COLUMN_NAME");
                FieldType fieldType;
                try {
                    fieldType = fromSql(dbColumns.getInt("DATA_TYPE"));
                } catch (IllegalStateException e) {
                    continue;
                }
                schemaFields.add(new SchemaField(columnName, fieldType, true));
            }
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
        return schemaFields.size() == 0 ? null : schemaFields;
    }

    @Override
    public List<SchemaField> createOrGetCollectionField(String project, String collection, List<SchemaField> fields) {
        if(collection.equals("public")) {
            throw new IllegalArgumentException("Collection name 'public' is not allowed.");
        }
        if(collection.startsWith("pg_") || collection.startsWith("_")) {
            throw new IllegalArgumentException("Collection names must not start with 'pg_' and '_' prefix.");
        }
        if(!collection.matches("^[a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Only alphanumeric characters allowed in collection name.");
        }

        try(Connection connection = connectionPool.getConnection()) {
            connection.setAutoCommit(false);
            ResultSet columns = connection.getMetaData().getColumns("", project, collection, null);
            List<SchemaField> currentFields = Lists.newArrayList();
            HashSet<String> strings = new HashSet<>();
            while (columns.next()) {
                String colName = columns.getString("COLUMN_NAME");
                strings.add(colName);
                currentFields.add(new Column(colName, fromSql(columns.getInt("DATA_TYPE")), true));
            }
            String query;
            if(currentFields.size() == 0) {
                String queryEnd = fields.stream().filter(f -> !strings.contains(f.getName()))
                        .map(f -> {
                            currentFields.add(f);
                            return f;
                        })
                        .map(f -> format("\"%s\" %s NULL", f.getName(), toSql(f.getType())))
                        .collect(Collectors.joining(", "));

                query = format("CREATE TABLE %s.%s (%s)", project, collection, queryEnd);
            }else {
                String queryEnd = fields.stream().filter(f -> !strings.contains(f.getName()))
                        .map(f -> {
                            currentFields.add(f);
                            return f;
                        })
                        .map(f -> format("ADD COLUMN \"%s\" %s NULL", f.getName(), toSql(f.getType())))
                        .collect(Collectors.joining(", "));

                query = format("ALTER TABLE %s.%s %s", project, collection, queryEnd);
            }

            connection.createStatement().execute(query);
            connection.commit();
            connection.setAutoCommit(true);
            return currentFields;
        } catch (SQLException e ) {
            // TODO: should we try again until this operation is done successfully, what about infinite loops?
            return createOrGetCollectionField(project, collection, fields);
        }
    }

    public static String toSql(FieldType type) {
        switch (type) {
            case LONG:
                return "BIGINT";
            case STRING:
                return "TEXT";
            case BOOLEAN:
            case DATE:
            case ARRAY:
            case TIME:
            case DOUBLE:
                return type.name();
            default:
                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }

    public static FieldType fromSql(int sqlType) {
        switch (sqlType) {
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.BIGINT:
                return FieldType.LONG;
            case Types.TINYINT:
            case Types.NUMERIC:
            case Types.INTEGER:
            case Types.SMALLINT:
                return FieldType.LONG;
            case Types.BOOLEAN:
            case Types.BIT:
                return FieldType.BOOLEAN;
            case Types.DATE:
                return FieldType.DATE;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return FieldType.TIME;
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.VARCHAR:
            case Types.OTHER:
                return FieldType.STRING;
            default:
                throw new IllegalStateException("sql type couldn't converted to fieldtype");
        }
    }
}
