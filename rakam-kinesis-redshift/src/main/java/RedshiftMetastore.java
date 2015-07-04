import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.rakam.analysis.ProjectNotExistsException;
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
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:23.
 */
public class RedshiftMetastore implements Metastore {
    RedshiftPoolDataSource connectionPool;

    @Inject
    public RedshiftMetastore(RedshiftPoolDataSource connectionPool) {
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
                if(schemaName.equals("information_schema") || schemaName.startsWith("pg_") || schemaName.equals("public")) {
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
            throw Throwables.propagate(e);
        }
        return table;
    }

    @Override
    public void createProject(String project) {
        checkProject(project);
        if(project.equals("information_schema")) {
            throw new IllegalArgumentException("information_schema is a reserved name for Postgresql backend.");
        }
        try(Connection connection = connectionPool.getConnection()) {
            connection.createStatement().execute("CREATE SCHEMA IF NOT EXISTS "+project);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<String> getProjects() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try(Connection connection = connectionPool.getConnection()) {
            ResultSet schemas = connection.getMetaData().getSchemas();
            while(schemas.next()) {
                String table_schem = schemas.getString("table_schem");
                if(!table_schem.equals("information_schema") && !table_schem.startsWith("pg_") && !table_schem.equals("public")) {
                    builder.add(table_schem);
                }
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        return builder.build();
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
    public List<SchemaField> createOrGetCollectionField(String project, String collection, List<SchemaField> fields) throws ProjectNotExistsException {
        if(collection.equals("public")) {
            throw new IllegalArgumentException("Collection name 'public' is not allowed.");
        }
        if(collection.startsWith("pg_") || collection.startsWith("_")) {
            throw new IllegalArgumentException("Collection names must not start with 'pg_' and '_' prefix.");
        }
        if(!collection.matches("^[a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Only alphanumeric characters allowed in collection name.");
        }

        List<SchemaField> currentFields = Lists.newArrayList();
        try(Connection connection = connectionPool.getConnection()) {
            connection.setAutoCommit(false);
            ResultSet columns = connection.getMetaData().getColumns("", project, collection, null);
            HashSet<String> strings = new HashSet<>();
            while (columns.next()) {
                String colName = columns.getString("COLUMN_NAME");
                strings.add(colName);
                currentFields.add(new Column(colName, fromSql(columns.getInt("DATA_TYPE")), true));
            }

            if(currentFields.size() == 0) {
                if(!getProjects().contains(project)) {
                    throw new ProjectNotExistsException();
                }
                String queryEnd = fields.stream().filter(f -> !strings.contains(f.getName()))
                        .map(f -> {
                            currentFields.add(f);
                            return f;
                        })
                        .map(f -> format("\"%s\" %s NULL", f.getName(), toSql(f.getType())))
                        .collect(Collectors.joining(", "));
                if(queryEnd.isEmpty()) {
                    return currentFields;
                }
                connection.createStatement().execute(format("CREATE TABLE %s.%s (%s)", project, collection, queryEnd));
            }else {
                List<SchemaField> collect = fields.stream().filter(f -> !strings.contains(f.getName()))
                        .map(f -> {
                            currentFields.add(f);
                            return f;
                        }).collect(Collectors.toList());
                for (SchemaField schemaField : collect) {
                    connection.createStatement().execute(format("ALTER TABLE %s.%s ADD COLUMN \"%s\" %s NULL",
                            project, collection, schemaField.getName(), toSql(schemaField.getType())));
                }
            }

            connection.commit();
            connection.setAutoCommit(true);
            return currentFields;
        } catch (SQLException e ) {
            // syntax error exception
            if(e.getSQLState().equals("42601") || e.getSQLState().equals("42939")) {
                throw new IllegalStateException("One of the column names is not valid because it collides with reserved keywords in Postgresql. : "+
                        (currentFields.stream().map(SchemaField::getName).collect(Collectors.joining(", "))) +
                        "See http://www.postgresql.org/docs/devel/static/sql-keywords-appendix.html");
            }else
                // column or table already exists
                if(e.getSQLState().equals("23505") || e.getSQLState().equals("42P07") || e.getSQLState().equals("42701") || e.getSQLState().equals("42710")) {
                    // TODO: should we try again until this operation is done successfully, what about infinite loops?
                    return createOrGetCollectionField(project, collection, fields);
                }else {
                    throw new IllegalStateException();
                }
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
                return type.name();
            case DOUBLE:
                return "double precision";
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
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return FieldType.TIMESTAMP;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
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
