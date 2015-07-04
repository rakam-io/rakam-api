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
import com.amazonaws.services.kinesis.connectors.redshift.RedshiftTransformer;
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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;

public class KinesisMessageModelRedshiftTransformer extends RedshiftTransformer<Event> {
    private final char delim;
    private final Metastore metastore;
    private final String collection;
    private final String project;

    /**
     * Creates a new KinesisMessageModelRedshiftTransformer.
     * 
     * @param config The configuration containing the Amazon Redshift data delimiter
     */
    public KinesisMessageModelRedshiftTransformer(Metastore metastore, KinesisConnectorConfiguration config) {
        super(Event.class);
        this.metastore = metastore;
        delim = config.REDSHIFT_DATA_DELIMITER;
        String[] projectCollection = config.KINESIS_INPUT_STREAM.split("_", 2);
        this.project = projectCollection[0];
        this.collection = projectCollection[1];
    }

    @Override
    public Event toClass(Record record) throws IOException {
        List<SchemaField> rakamSchema = metastore.getCollection(project, collection);
        if(rakamSchema == null) {
            throw new IllegalStateException("metadata server screwed up");
        }
        Schema avroSchema = convertAvroSchema(rakamSchema);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(record.getData().array(), null);
        new DynamicDatumReader<>(avroSchema).read(avroRecord, binaryDecoder);
        return Event.create(project, record.getPartitionKey(), avroRecord);
    }

    @Override
    public String toDelimitedString(Event record) {
        GenericRecord properties = record.properties();
        int size = properties.getSchema().getFields().size() - 1;
        StringBuilder b = new StringBuilder();

        for (int i = 0; i < size; i++) {
            Object obj = properties.get(i);
            if(obj != null) {
                b.append(obj);
            }
            b.append(delim);
        }
        b.append(properties.get(size))
                .append("\n");

        return b.toString();
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

}
