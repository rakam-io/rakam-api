import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericData;
import org.rakam.collection.FieldType;

import static org.rakam.util.AvroUtil.getAvroSchema;

public class TestClickHouse {
    private Object generateSample(FieldType type) {
        switch (type) {
            case STRING:
                return "test";
            case BINARY:
                return new byte[]{1};
            case DECIMAL:
            case DOUBLE:
                return 1.0;
            case BOOLEAN:
                return true;
            case INTEGER:
            case DATE:
            case TIME:
                return 1;
            case LONG:
            case TIMESTAMP:
                return 1L;
            default:
                if (type.isArray()) {
                    FieldType arrayElementType = type.getArrayElementType();
                    return new GenericData.Array(getAvroSchema(type), ImmutableList.of(generateSample(arrayElementType)));
                }
                if (type.isMap()) {
                    return ImmutableMap.of("test", generateSample(type.getMapValueType()));
                } else {
                    throw new IllegalArgumentException();
                }
        }
    }
}
