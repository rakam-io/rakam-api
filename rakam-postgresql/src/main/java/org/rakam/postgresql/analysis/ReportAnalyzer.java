package org.rakam.postgresql.analysis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.util.JsonHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.format.DateTimeFormatter;

public final class ReportAnalyzer {

    private ReportAnalyzer() throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static ObjectNode execute(Connection conn, String sql) throws SQLException {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            ResultSetMetaData metaData = rs.getMetaData();
            ArrayNode metadata = getMetadata(metaData);

            ObjectNode jsonNodes = JsonHelper.jsonObject();


                ArrayNode array = JsonHelper.jsonArray();

                while (rs.next()) {
                    array.add(getRowFromResultSet(rs, metaData));
                }

                jsonNodes.set("result", array);
                jsonNodes.put("total", array.size());


            jsonNodes.set("metadata", metadata);

            return jsonNodes;
    }

    private static ArrayNode getMetadata(ResultSetMetaData metaData) throws SQLException {
        ArrayNode columns = JsonHelper.jsonArray();
        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
            ObjectNode metadata = JsonHelper.jsonObject();
            String columnName = metaData.getColumnName(i);
            metadata.put("name", columnName);
            metadata.put("position", i);

            switch (metaData.getColumnType(i)) {
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.SMALLINT:
                case Types.DOUBLE:
                    metadata.put("type", "numeric");
                    break;
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                case Types.CHAR:
                    metadata.put("type", "string");
                    break;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    metadata.put("type", "date");
                    break;
                case Types.NULL:
                    metadata.put("type", "null");
                    break;
                case Types.ARRAY:
                    metadata.put("type", "array");
                    break;
                case Types.BOOLEAN:
                    metadata.put("type", "boolean");
                    break;
                default:
                    continue;
            }
            columns.add(metadata);
        }
        return columns;
    }

    private static ObjectNode getRowFromResultSet(ResultSet resultSet, ResultSetMetaData metaData) throws SQLException {
        ObjectNode objectNode = JsonHelper.jsonObject();

        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; ++i) {
            String columnName = metaData.getColumnName(i).toLowerCase();

            switch (metaData.getColumnType(i)) {
                case Types.INTEGER:
                    objectNode.put(columnName, resultSet.getInt(i));
                    break;
                case Types.BIGINT:
                    objectNode.put(columnName, resultSet.getLong(i));
                    break;
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                    objectNode.put(columnName, resultSet.getString(i));
                    break;
                case Types.DATE:
                    objectNode.put(columnName, resultSet.getDate(i).toString());
                    break;
                case Types.DOUBLE:
                    objectNode.put(columnName, resultSet.getDouble(i));
                    break;
                case Types.CHAR:
                    objectNode.put(columnName, resultSet.getString(i));
                    break;
                case Types.NULL:
                    objectNode.putNull(columnName);
                    break;
                case Types.TIME:
                    objectNode.put(columnName, resultSet.getTime(i).toString());
                    break;
                case Types.ARRAY:
                    Object array = resultSet.getArray(i).getArray();
                    objectNode.set(columnName, JsonHelper.jsonArray());
                    break;
                case Types.TIMESTAMP:
                    objectNode.put(columnName, DateTimeFormatter.ISO_INSTANT.format(resultSet.getTimestamp(i).toInstant()));
                    break;
                case Types.BOOLEAN:
                    objectNode.put(columnName, resultSet.getBoolean(i));
                    break;
                default:
                    continue;
            }
        }
        return objectNode;
    }

    private static void extractColumnsFromResultSet(ResultSet resultSet, ResultSetMetaData metaData, JsonNode object) throws SQLException {
        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
            ArrayNode column = (ArrayNode) object.get(metaData.getColumnName(i));
            int type = metaData.getColumnType(i);

            switch (type) {
                case Types.INTEGER:
                    column.add(resultSet.getInt(i));
                    break;
                case Types.BIGINT:
                    column.add(resultSet.getLong(i));
                    break;
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                    column.add(resultSet.getString(i));
                    break;
                case Types.DATE:
                    column.add(resultSet.getDate(i).toString());
                    break;
                case Types.DOUBLE:
                    column.add(resultSet.getDouble(i));
                    break;
                case Types.CHAR:
                    column.add(resultSet.getString(i));
                    break;
                case Types.NULL:
                    column.addNull();
                    break;
                case Types.TIME:
                    column.add(resultSet.getTime(i).toString());
                    break;
                case Types.ARRAY:
                    Object array = resultSet.getArray(i).getArray();
                    column.add(JsonHelper.jsonArray());
                    break;
                case Types.TIMESTAMP:
                    column.add(resultSet.getTimestamp(i).toLocalDateTime().toString());
                    break;
                case Types.BOOLEAN:
                    column.add(resultSet.getBoolean(i));
                    break;
                default:
                    continue;
            }
        }
    }

}
