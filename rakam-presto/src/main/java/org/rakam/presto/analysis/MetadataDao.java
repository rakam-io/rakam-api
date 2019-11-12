package org.rakam.presto.analysis;

import com.facebook.presto.raptor.metadata.SchemaTableNameMapper;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public interface MetadataDao {
    class TableColumn {
        private final SchemaTableName table;
        private final String columnName;
        private final Type dataType;

        public TableColumn(SchemaTableName table, String columnName, Type dataType) {
            this.table = requireNonNull(table, "table is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.dataType = requireNonNull(dataType, "dataType is null");
        }


        public SchemaTableName getTable() {
            return table;
        }

        public String getColumnName() {
            return columnName;
        }

        public Type getDataType() {
            return dataType;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                    .add("columnName", columnName)
                    .add("dataType", dataType)
                    .toString();
        }

        public static class Mapper
                implements ResultSetMapper<TableColumn> {
            private final TypeManager typeManager;

            @Inject
            public Mapper(TypeManager typeManager) {
                this.typeManager = requireNonNull(typeManager, "typeManager is null");
            }

            @Override
            public TableColumn map(int index, ResultSet r, StatementContext ctx)
                    throws SQLException {
                SchemaTableName table = new SchemaTableName(
                        r.getString("schema_name"),
                        r.getString("table_name"));

                String typeName = r.getString("data_type");
                Type type = typeManager.getType(parseTypeSignature(typeName));
                checkArgument(type != null, "Unknown type %s", typeName);

                return new TableColumn(
                        table,
                        r.getString("column_name"),
                        type);
            }
        }
    }

    String TABLE_COLUMN_SELECT = "" +
            "SELECT t.schema_name, t.table_name,\n" +
            "  c.column_id, c.column_name, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n";

    @SqlQuery("SELECT schema_name, table_name\n" +
            "FROM tables\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)")
    @Mapper(SchemaTableNameMapper.class)
    List<SchemaTableName> listTables(
            @Bind("schemaName") String schemaName);

    @SqlQuery("SELECT DISTINCT schema_name FROM tables")
    List<String> listSchemaNames();

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY column_id")
    List<TableColumn> listTableColumns(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);


    @SqlUpdate("INSERT INTO tables (\n" +
            "  schema_name, table_name, table_version, create_time)\n" +
            "VALUES (\n" +
            "  :schemaName, :tableName, :tableVersion, :createTime)\n")
    @GetGeneratedKeys
    long insertTable(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName,
            @Bind("tableVersion") long tableVersion,
            @Bind("createTime") long createTime);


    @SqlUpdate("INSERT INTO columns (table_id, column_name, data_type, create_time)\n" +
            "VALUES ((SELECT table_id FROM tables WHERE schema_name = :schemaName AND table_name = :tableName), :columnName, :dataType, :createTime)")
    void insertColumn(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName,
            @Bind("columnName") String columnName,
            @Bind("dataType") String dataType,
            @Bind("createTime") long createTime);
}
