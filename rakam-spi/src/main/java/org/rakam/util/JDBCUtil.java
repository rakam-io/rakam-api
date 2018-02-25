package org.rakam.util;

import org.rakam.collection.FieldType;

import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class JDBCUtil {
    private static final Map<String, FieldType> REVERSE_TYPE_MAP = Arrays.asList(FieldType.values()).stream()
            .collect(Collectors.toMap(JDBCUtil::toSql, a -> a));

    public static FieldType getType(String name) {
        FieldType fieldType = REVERSE_TYPE_MAP.get(name.toUpperCase());
        if (fieldType == null) {
            throw new IllegalArgumentException(String.format("type %s couldn't recognized.", name));
        }
        return fieldType;
    }

    public static String toSql(FieldType type) {
        switch (type) {
            case INTEGER:
                return "INT";
            case DECIMAL:
                return "DECIMAL";
            case LONG:
                return "BIGINT";
            case STRING:
                return "VARCHAR";
            case BINARY:
                return "VARBINARY";
            case BOOLEAN:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return type.name();
            case DOUBLE:
                return "DOUBLE";
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

    public static FieldType fromSql(int sqlType, String typeName) {
        return fromSql(sqlType, typeName, name -> {
            if (name.startsWith("_")) {
                if (name.startsWith("_int")) {
                    return FieldType.ARRAY_LONG;
                }
                if (name.equals("_bool")) {
                    return FieldType.ARRAY_BOOLEAN;
                }
                if (name.equals("_text") || name.equals("_varchar")) {
                    return FieldType.ARRAY_STRING;
                }
                if (name.startsWith("_float")) {
                    return FieldType.ARRAY_DOUBLE;
                }
            }
            if (name.equals("jsonb")) {
                return FieldType.MAP_STRING;
            }
            if (name.equals("json")) {
                return FieldType.STRING;
            }
            if (name.equals("unknown")) {
                return FieldType.STRING;
            }

            throw new UnsupportedOperationException(format("type '%s' is not supported.", typeName));
        });
    }

    public static FieldType fromSql(int sqlType, String typeName, Function<String, FieldType> arrayTypeNameMapper) {
        switch (sqlType) {
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.LONGVARBINARY:
                return FieldType.BINARY;
            case Types.BIGINT:
                return FieldType.LONG;
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.SMALLINT:
                return FieldType.INTEGER;
            case Types.DECIMAL:
                return FieldType.DECIMAL;
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
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.REAL:
                return FieldType.DOUBLE;
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.VARCHAR:
                return FieldType.STRING;
            case Types.OTHER:
                if (typeName.equals("citext")) {
                    return FieldType.STRING;
                }
                if (typeName.equals("jsonb")) {
                    return FieldType.MAP_STRING;
                }
                if (typeName.equals("json")) {
                    return FieldType.STRING;
                }
            default:
                return arrayTypeNameMapper.apply(typeName);
        }
    }
}
