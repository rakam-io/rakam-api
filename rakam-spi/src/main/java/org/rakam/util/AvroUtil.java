/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.util;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.node.NullNode;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.UNION;

public final class AvroUtil {
    static {
        try {
            Field validateNames = Schema.class.getDeclaredField("validateNames");
            boolean accessible = validateNames.isAccessible();
            validateNames.setAccessible(true);
            validateNames.set(null, new ThreadLocal<Boolean>() {
                @Override
                public Boolean get() {
                    return false;
                }

                @Override
                public void set(Boolean value) {
                    // no-op
                    return;
                }
            });
            if (!accessible) {
                validateNames.setAccessible(false);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private AvroUtil()
            throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static Schema convertAvroSchema(List<SchemaField> fields, Map<String, List<SchemaField>> conditionalMagicFields) {
        List<Schema.Field> avroFields = fields.stream()
                .map(AvroUtil::generateAvroField).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);

        conditionalMagicFields.keySet().stream()
                .filter(s -> !avroFields.stream().anyMatch(af -> af.name().equals(s)))
                .map(n -> new Schema.Field(n, Schema.create(NULL), "", null))
                .forEach(x -> avroFields.add(x));

        schema.setFields(avroFields);
        return schema;
    }

    public static Schema convertAvroSchema(Collection<SchemaField> fields) {
        List<Schema.Field> avroFields = fields.stream()
                .map(AvroUtil::generateAvroField).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);
        schema.setFields(avroFields);

        return schema;
    }

    public static Schema.Field generateAvroField(SchemaField field) {
        return new Schema.Field(field.getName(), generateAvroSchema(field.getType()), null, NullNode.getInstance());
    }

    public static Schema generateAvroSchema(FieldType field) {
        return Schema.createUnion(Lists.newArrayList(Schema.create(NULL), getAvroSchema(field)));
    }

    public static void put(GenericRecord properties, String key, Object value) {
        Schema union = properties.getSchema().getField(key).schema();
        Schema.Type type;
        if (union.getType().equals(UNION)) {
            type = union.getTypes().get(1).getType();
        } else {
            type = union.getType();
        }
        properties.put(key, AvroUtil.cast(type, value));
    }

    public static Object cast(Schema.Type type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case BYTES:
                return value instanceof byte[] ? value : null;
            case INT:
                if (value instanceof Integer) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                try {
                    return Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            case LONG:
                if (value instanceof Long) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                try {
                    return Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            case FLOAT:
                if (value instanceof Float) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                try {
                    return Float.parseFloat(value.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            case DOUBLE:
                if (value instanceof Double) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                try {
                    return Double.parseDouble(value.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof String) {
                    return value.equals("true");
                }
            case STRING:
                if (value instanceof String) {
                    return value;
                }
                return value.toString();
            case ARRAY:
                if (value instanceof List) {
                    return value;
                }
                return null;
            case MAP:
                if (value instanceof Map) {
                    return value;
                }
                return null;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static Schema getAvroSchema(FieldType type) {
        switch (type) {
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case BINARY:
                return Schema.create(Schema.Type.BYTES);
            case DOUBLE:
            case DECIMAL:
                return Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case DATE:
            case TIME:
            case INTEGER:
                return Schema.create(Schema.Type.INT);
            case LONG:
            case TIMESTAMP:
                return Schema.create(Schema.Type.LONG);
            default:
                if (type.isMap()) {
                    Schema union = Schema.createUnion(ImmutableList.of(Schema.create(Schema.Type.NULL), getAvroSchema(type.getMapValueType())));
                    return Schema.createMap(union);
                }
                if (type.isArray()) {
                    Schema union = Schema.createUnion(ImmutableList.of(Schema.create(Schema.Type.NULL), getAvroSchema(type.getArrayElementType())));
                    return Schema.createArray(union);
                }
                throw new IllegalStateException();
        }
    }
}
