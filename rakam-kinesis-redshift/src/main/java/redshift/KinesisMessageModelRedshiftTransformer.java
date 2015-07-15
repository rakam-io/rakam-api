/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package redshift;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.codehaus.jackson.node.NullNode;
import org.rakam.analysis.DynamicDatumReader;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.util.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;

public class KinesisMessageModelRedshiftTransformer {
    private final char delim;
    private final Metastore metastore;
    private Map<Tuple<String, String>, Schema> schemaCache;

    /**
     * Creates a new KinesisMessageModelRedshiftTransformer.
     *
     * @param config The configuration containing the Amazon Redshift data delimiter
     */
    public KinesisMessageModelRedshiftTransformer(Metastore metastore, KinesisConnectorConfiguration config) {
        this.metastore = metastore;
        delim = config.REDSHIFT_DATA_DELIMITER;
        schemaCache = new ConcurrentHashMap<>();
    }

    public void clearSchemaCache() {
        schemaCache.clear();
    }

    public Event fromClass(Record record) throws IOException {
        ByteBuffer data = record.getData();
        byte[] projectBytes = new byte[readInt(data)];
        data.get(projectBytes);

        byte[] collectionBytes = new byte[readInt(data)];
        data.get(collectionBytes);

        String project = new String(projectBytes);
        String collection = new String(collectionBytes);

        Schema avroSchema = schemaCache.get(new Tuple<>(project, collection));
        if(avroSchema == null) {
            avroSchema = updateAndGetSchema(project, collection);
        }

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(data.array(), data.position(), data.remaining(), null);
        new DynamicDatumReader<>(avroSchema).read(avroRecord, binaryDecoder);
        return Event.create(project, collection, avroRecord);
    }

    private Schema updateAndGetSchema(String project, String collection) {
        List<SchemaField> rakamSchema = metastore.getCollection(project, collection);
        if(rakamSchema == null) {
            throw new IllegalStateException("metadata server screwed up");
        }
        Schema avroSchema = convertAvroSchema(rakamSchema);
        schemaCache.put(new Tuple<>(project, collection), avroSchema);
        return avroSchema;
    }

    private static Schema.Field generateAvroSchema(SchemaField field) {
        Schema es;
        if (field.isNullable()) {
            es = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), getAvroSchema(field.getType())));
            return new Schema.Field(field.getName(), es, null, NullNode.getInstance());
        } else {
            es = getAvroSchema(field.getType());
            return new Schema.Field(field.getName(), es, null, null);
        }
    }

    public static Schema convertAvroSchema(List<SchemaField> fields) {
        List<Schema.Field> avroFields = fields.stream()
                .map(KinesisMessageModelRedshiftTransformer::generateAvroSchema).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);
        schema.setFields(avroFields);
        return schema;
    }

    private static Schema getAvroSchema(FieldType type) {
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

    private static int readInt(ByteBuffer buffer) throws IOException {
        int b = buffer.get() & 0xff;
        int n = b & 0x7f;
        if (b > 0x7f) {
            b = buffer.get() & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buffer.get() & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buffer.get() & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = buffer.get() & 0xff;
                        n ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            throw new IOException("Invalid int encoding");
                        }
                    }
                }
            }
        }
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

}
