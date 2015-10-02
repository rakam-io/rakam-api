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

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;


public class AvroUtil {
    public static Schema convertAvroSchema(List<SchemaField> fields) {
        List<Schema.Field> avroFields = fields.stream()
                .map(AvroUtil::generateAvroSchema).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);
        schema.setFields(avroFields);
        return schema;
    }

    public static Schema.Field generateAvroSchema(SchemaField field) {
        Schema es;
        if (field.isNullable()) {
            es = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), getAvroSchema(field.getType())));
            return new Schema.Field(field.getName(), es, null, NullNode.getInstance());
        } else {
            es = getAvroSchema(field.getType());
            return new Schema.Field(field.getName(), es, null, null);
        }
    }


    public static Schema getAvroSchema(FieldType type) {
        switch (type) {
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case ARRAY:
                return Schema.create(Schema.Type.ARRAY);
            case LONG:
                return Schema.create(Schema.Type.LONG);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case DATE:
                return Schema.create(Schema.Type.INT);
            case HYPERLOGLOG:
                return Schema.create(Schema.Type.BYTES);
            case TIME:
                return Schema.create(Schema.Type.LONG);
            default:
                throw new IllegalStateException();
        }
    }
}
