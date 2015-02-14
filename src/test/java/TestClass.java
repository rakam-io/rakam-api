import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.codehaus.jackson.node.NullNode;
import org.junit.Test;
import org.rakam.util.JsonHelper;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/02/15 23:57.
 */
public class TestClass {
    @Test
    public void test() throws InterruptedException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        String source = "package test; public class Test { static { System.out.println(\"hello\"); } public Test() { System.out.println(\"world\"); } }";

        File tempFile = File.createTempFile("temp", ".tmp");

        System.out.println(tempFile);
        File root = tempFile.getParentFile();
        File sourceFile = new File(root, "test/Test.java");
        sourceFile.getParentFile().mkdirs();
        new FileWriter(sourceFile).append(source).close();

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, sourceFile.getPath());

        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{root.toURI().toURL()});
        Class<?> cls = Class.forName("test.Test", true, classLoader); // Should print "hello".
        Object instance = cls.newInstance(); // Should print "world".
        System.out.println(instance); // Should print "test.Test@hashcode"
    }

    @Test
    public void test2() throws IOException {
        ArrayList fields = Lists.newArrayList(
                new Schema.Field("project", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("naber", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("emre", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("emrme", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))), null, NullNode.getInstance()),
                new Schema.Field("halil", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("fazıl", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("fazılz", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance())
        );
        Schema schema = Schema.createRecord("rakam", null, "avro.test", false);
        schema.setFields(fields);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ObjectMapper objectMapper = new ObjectMapper(new AvroFactory());
        ObjectNode put = JsonHelper.jsonObject()
                .put("naber", "naber")
                .put("project", "project")
                .put("halil", "project")
                .put("fazıl", "project")
                .put("fazılz", "project")
                .put("emrme", 4L)
                .put("emre", "olumemre");

        objectMapper.writer(new AvroSchema(schema)).writeValue(out, put);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(out.toByteArray());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);

        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        GenericData.Record read = new GenericData.Record(schema);

        for (int i = 0; i < 10_000; i++) {
            read = reader.read(read, decoder);
            byteArrayInputStream.reset();
        }

        long l = System.currentTimeMillis();
        for (int i = 0; i < 20_000_000; i++) {
            read = reader.read(read, decoder);
            byteArrayInputStream.reset();
        }
        System.out.println(System.currentTimeMillis() - l);
    }

    @Test
    public void testdsda2() throws IOException {
        ArrayList fields = Lists.newArrayList(
                new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
                new Schema.Field("name", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("email0", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("email1", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("email2", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("email3", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance())
        );
        Schema schema = Schema.createRecord("rakam", null, "avro.test", false);
        schema.setFields(fields);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ObjectMapper objectMapper = new ObjectMapper(new AvroFactory());
        ObjectNode put = JsonHelper.jsonObject()
                .put("id", 53453)
                .put("name", "fssd")
                .put("email0", "fssd")
                .put("email1", "fssd")
                .put("email2", "fssd")
                .put("email3", "fssd");

        objectMapper.writer(new AvroSchema(schema)).writeValue(out, put);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(out.toByteArray());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);

        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        GenericData.Record read = new GenericData.Record(schema);

        for (int i = 0; i < 10_000; i++) {
            read = reader.read(read, decoder);
            byteArrayInputStream.reset();
        }

        long l = System.currentTimeMillis();
        for (int i = 0; i < 20_000_000; i++) {
            read = reader.read(read, decoder);
            byteArrayInputStream.reset();
        }
        System.out.println(System.currentTimeMillis() - l);
    }

    public static void main(String[] args) throws IOException {
        ArrayList fields = Lists.newArrayList(
                new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
                new Schema.Field("name", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("email0", Schema.create(Schema.Type.STRING), null, org.codehaus.jackson.node.TextNode.valueOf("")),
                new Schema.Field("email1", Schema.create(Schema.Type.STRING), null, org.codehaus.jackson.node.TextNode.valueOf("")),
                new Schema.Field("email2", Schema.create(Schema.Type.STRING), null, org.codehaus.jackson.node.TextNode.valueOf("")),
                new Schema.Field("email3", Schema.create(Schema.Type.STRING), null, org.codehaus.jackson.node.TextNode.valueOf(""))
        );
        Schema schema = Schema.createRecord("rakam", null, "avro.test", false);
        schema.setFields(fields);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ObjectMapper objectMapper = new ObjectMapper(new AvroFactory());
        ObjectNode put = JsonHelper.jsonObject()
                .put("id", 53453)
                .put("name", "fssd")
                .put("email0", "fssd")
                .put("email1", "fssd")
                .put("email2", "fssd")
                .put("email3", "fssd");
//
//        ByteArrayOutputStream s = new ByteArrayOutputStream();
//        s.write(put.toString().getBytes());
//        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, s);
//        SpecificDatumWriter<Example> w = new SpecificDatumWriter<>(Example.class);

        objectMapper.writer(new AvroSchema(schema)).writeValue(out, put);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(out.toByteArray());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);

//        SpecificDatumReader<Example> reader = new SpecificDatumReader<>(Example.class);
//
//        Example read = new Example();

//        for (int i = 0; i < 10_000; i++) {
//            read = reader.read(read, decoder);
//            byteArrayInputStream.reset();
//        }

        long l = System.currentTimeMillis();
        for (int i = 0; i < 20_000_000; i++) {
//            read = reader.read(read, decoder);
            byteArrayInputStream.reset();
        }
        System.out.println(System.currentTimeMillis() - l);
    }

    @Test
    public void test4() throws IOException {
        ArrayList fields = Lists.newArrayList(
                new Schema.Field("project", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("naber", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("emre", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("emrme", Schema.createUnion(Lists.newArrayList( Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))), null, NullNode.getInstance()),
                new Schema.Field("halil", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("fazıl", Schema.createUnion(Lists.newArrayList( Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("fazılz", Schema.createUnion(Lists.newArrayList( Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance())
        );
        Schema schema = Schema.createRecord("ProjectName_CollectionName", null, "org..test", false);
        schema.setFields(fields);

        File tempFile = File.createTempFile("temp", ".tmp");
//        SpecificCompiler specificCompiler = new SpecificCompiler(schema);
//        specificCompiler.compileToDestination(null, tempFile.getParentFile());
    }

    @Test
    public void ff() throws IOException, InterruptedException {
        String jsonStr = "{\"project\": \"emre0\", \"collection\": \"pageView2\", \"url\": \"http://google.com\", \"ff\": 3, \"dfsdf\": 334, \"fsdfsdf\": \"fgdfdfgdfgdfg\"}";
        for (int i =0; i<10_000; i++) {
            JsonNode read = JsonHelper.read(jsonStr);
            read.isObject();
        }
        Thread.sleep(500);

        long l = System.currentTimeMillis();
        for (int i =0; i<20_000_000; i++) {
            JsonNode read = JsonHelper.read(jsonStr);
            read.isObject();
        }
        System.out.println(System.currentTimeMillis() - l);
    }



    @Test
    public void ffs() throws IOException, InterruptedException {
        SimpleModule module = new SimpleModule();
        JsonDeserializer<GenericData.Record> customDeserializer = new AvroRecordDeserializer(null);
        module.addDeserializer(GenericData.Record.class, customDeserializer);

        ArrayList fields = Lists.newArrayList(
                new Schema.Field("project", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("collection", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("url", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance()),
                new Schema.Field("ff", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))), null, NullNode.getInstance()),
                new Schema.Field("dfsdf", Schema.createUnion(Lists.newArrayList( Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))), null, NullNode.getInstance()),
                new Schema.Field("fsdfsdf", Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, NullNode.getInstance())
                );
        Schema schema = Schema.createRecord(fields);

        String jsonStr = "{\"project\": \"emre0\", \"collection\": \"pageView2\", \"url\": \"http://google.com\", \"ff\": 3, \"dfsdf\": 334, \"fsdfsdf\": \"fgdfdfgdfgdfg\"}";
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module);

        mapper.readTree(jsonStr);

        ObjectWriter writer = mapper.writer(new AvroSchema(schema));

        JsonNode read = JsonHelper.read(jsonStr);
        for (int i =0; i<10_000; i++) {
            writer.writeValueAsBytes(read);
        }
        Thread.sleep(500);


        long l = System.currentTimeMillis();
        for (int i =0; i<10_000_000; i++) {
            writer.writeValueAsBytes(read);
        }
        System.out.println(System.currentTimeMillis() - l);
    }
}