package org.rakam.presto;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import org.rakam.collection.FieldType;

import static com.facebook.presto.type.MapParametricType.MAP;

public class PrestoType {
    private static final TypeManager defaultTypeManager = MetadataManager.createTestMetadataManager().getTypeManager();

    public static Type toType(FieldType type) {
        switch (type) {
            case DOUBLE:
                return DoubleType.DOUBLE;
            case LONG:
                return BigintType.BIGINT;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case STRING:
                return VarcharType.VARCHAR;
            case INTEGER:
                return IntegerType.INTEGER;
            case DECIMAL:
                return DecimalType.createDecimalType();
            case DATE:
                return DateType.DATE;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case TIME:
                return TimeType.TIME;
            case BINARY:
                return VarbinaryType.VARBINARY;
            default:
                if (type.isArray()) {
                    return new ArrayType(toType(type.getArrayElementType()));
                }
                if (type.isMap()) {
                    return MAP.createType(defaultTypeManager, ImmutableList.of(TypeParameter.of(VarcharType.VARCHAR),
                            TypeParameter.of(toType(type.getMapValueType()))));
                }
                throw new IllegalStateException();
        }
    }
}
