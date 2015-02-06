package org.rakam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.node.NullNode;
import org.rakam.collection.event.CustomGenericDatumReader;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.server.WebServer;
import org.rakam.util.JsonHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {

    public static byte[] jsonToAvro(String json, Schema schema) throws IOException {
        DataFileWriter<Object> writer;
        ByteArrayOutputStream output;
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        output = new ByteArrayOutputStream();
        writer = new DataFileWriter<>(new GenericDatumWriter<>());
        writer.create(schema, output);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
        Object datum;

//        BinaryEncoder encoder = EncoderFactory.get().jsonEncoder(outputStream, null);


        JsonEncoder jsonEncoder =  EncoderFactory.get().jsonEncoder(schema, new ByteArrayOutputStream());
//        Json.write(JsonHelper.jsonObject(), jsonEncoder);
        while (true) {
            try {
                datum = reader.read(null, decoder);
            } catch (EOFException eof) {
                break;
            }
            writer.append(datum);
        }
        writer.flush();
        return output.toByteArray();
    }

    public static Type getAvroType(JsonNodeType jsonType) {
        switch (jsonType) {
            case NUMBER:
                return Type.FLOAT;
            case STRING:
                return Type.STRING;
            case BOOLEAN:
                return Type.BOOLEAN;
            case NULL:
                return Type.NULL;
            case OBJECT:
                return Type.RECORD;
            case ARRAY:
                return Type.ARRAY;
            default:
                throw new UnsupportedOperationException("unsupported json field type");
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                System.out.println("exiting..");
//                TODO: gracefully exit.
//                System.exit(0);
//            }
//        });


        ArrayList fields = new ArrayList();
        fields.add(new Schema.Field("project", Schema.create(Type.STRING), null, null));
        fields.add(new Schema.Field("naber", Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("emre", Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("emrme", Schema.createUnion(Lists.newArrayList( Schema.create(Type.NULL), Schema.create(Type.INT))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("halil", Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("fazıl", Schema.createUnion(Lists.newArrayList( Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        Schema schema = Schema.createRecord("rakam", null, "avro.test", false);
        schema.setFields(fields);

        SpecificCompiler specificCompiler = new SpecificCompiler(schema);
        specificCompiler.compileToDestination();


        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ObjectMapper objectMapper = new ObjectMapper(new AvroFactory());
        ObjectNode put = JsonHelper.jsonObject()
                .put("naber", "naber")
                .put("project", "project")
                .put("halil", "project")
                .put("fazıl", "project")
                .put("emrme", 4L)
                .put("emre", "olumemre");

        objectMapper.writer(new AvroSchema(schema)).writeValue(out, put);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(out.toByteArray());

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);

        fields = new ArrayList();
        fields.add(new Schema.Field("project", Schema.create(Type.STRING), null, null));
        fields.add(new Schema.Field("naber", Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("emre", Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("emrme", Schema.createUnion(Lists.newArrayList( Schema.create(Type.NULL), Schema.create(Type.INT))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("halil", Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("fazıl", Schema.createUnion(Lists.newArrayList( Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        fields.add(new Schema.Field("fazılz", Schema.createUnion(Lists.newArrayList( Schema.create(Type.NULL), Schema.create(Type.STRING))), null, NullNode.getInstance()));
        schema = Schema.createRecord("rakam", null, "avro.test", false);
        schema.setFields(fields);

        CustomGenericDatumReader<GenericData.Record> reader = new CustomGenericDatumReader<>(schema);

        for (int i = 0; i < 10000; i++) {
            GenericData.Record result = reader.read(null, decoder);
            byteArrayInputStream.reset();
        }

        long l = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            GenericData.Record result = reader.read(null, decoder);
            byteArrayInputStream.reset();
        }
        System.out.println(System.currentTimeMillis() - l);

//        ObjectReader read = objectMapper.reader(ObjectNode.class)
//                .with(new AvroSchema(schema));
//
//        for (int i = 0; i < 10000; i++) {
//            ObjectNode result = read.readValue(byteArrayInputStream);
//            byteArrayInputStream.reset();
//        }
//
//        long l = System.currentTimeMillis();
//        for (int i = 0; i < 10000000; i++) {
//            ObjectNode result = read.readValue(byteArrayInputStream);
//            byteArrayInputStream.reset();
//        }
//        System.out.println(System.currentTimeMillis() - l);

        System.exit(0);

//        GenericData.Record record = new GenericRecordBuilder(schema)
//            .set("project", "ff").build();
//
//        GenericDatumWriter w = new GenericDatumWriter(schema);
//        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
//
//        w.write(record, encoder);
//        encoder.flush();
//
//        Schema newSchema = SchemaBuilder.record("test").fields()
//                .requiredString("project")
//                .optionalString("city")
//                .optionalString("newField")
//                .endRecord();
//
//        DatumReader<GenericRecord> reader = new GenericDatumReader<>(newSchema);
//        Decoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
//        GenericRecord result = reader.read(null, decoder);
//
//        List<Field> f = new ArrayList<>();
//        f.add(new Field("project", Schema.create(Type.STRING), null, null));
//        f.add(new Field("city", Schema.createUnion(Lists.newArrayList(Schema.create(Type.STRING), Schema.create(Type.NULL))), null, NullNode.getInstance()));
//        f.add(new Field("city2", Schema.createUnion(Lists.newArrayList(Schema.create(Type.STRING), Schema.create(Type.NULL))), null, NullNode.getInstance()));
//
//        Schema s = Schema.createRecord("Address", null, "avro.test", false);
//        s.setFields(f);
//
//        ObjectNode empl = null;
//        try {
//            empl = mapper.reader(ObjectNode.class)
//                    .with(new AvroSchema(s))
//                            .readValue(avroData);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        Cluster cluster = new ClusterBuilder().start();

        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("partitioner.class", "org.rakam.collection.event.KafkaPartitioner");

        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer(config);

        Injector injector = Guice.createInjector(new ServiceRecipe(cluster, producer));

        try {
            injector.getInstance(WebServer.class).run(9999);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


