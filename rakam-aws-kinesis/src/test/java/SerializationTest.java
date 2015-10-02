import org.junit.Test;

import java.io.IOException;

public class SerializationTest {

    @Test
    public void test() throws IOException {
//        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BigintType.BIGINT, VarcharType.VARCHAR, VarcharType.VARCHAR));
//        List<Schema.Field> test = ImmutableList.of(
//                new Schema.Field("test", Schema.create(Schema.Type.LONG), null, null),
//                new Schema.Field("test1", Schema.create(Schema.Type.STRING), null, null),
//                new Schema.Field("test2", Schema.create(Schema.Type.STRING), null, null));
//        Schema schema = Schema.createRecord(test);
//
//        ByteArrayOutputStream output = new ByteArrayOutputStream();
//        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
//        GenericDatumWriter writer = new GenericDatumWriter(schema);
//        GenericData.Record record = new GenericData.Record(schema);
//        record.put("test", 10L);
//        record.put("test1", "tamam");
//        record.put("test2", "tamam2");
//        writer.write(record, encoder);
//        encoder.flush();

//        PageDatumReader pageDatumReader = new PageDatumReader(pageBuilder, schema);
//
//        byte[] bytes = output.toByteArray();
//
//        DecoderFactory decoderFactory = DecoderFactory.get();
//        BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(bytes, 0, bytes.length, null);
//
//        for (int i = 0; i < 5000; i++) {
//            binaryDecoder = decoderFactory.binaryDecoder(bytes, 0, bytes.length, binaryDecoder);
//
//            pageDatumReader.read(null, binaryDecoder);
//        }
//
//        long l = System.currentTimeMillis();
//        for (int i = 0; i < 10000000; i++) {
//            binaryDecoder = decoderFactory.binaryDecoder(bytes, 0, bytes.length, binaryDecoder);
//            pageDatumReader.read(null, binaryDecoder);
//        }
//        System.out.println(System.currentTimeMillis()-l);
    }

    @Test
    public void testNadme() throws Exception {

//        ImmutableList<AbstractType> of = ImmutableList.of(DoubleType.DOUBLE);
//        for (AbstractType abstractType : of) {
//            BlockBuilder blockBuilder = abstractType.createBlockBuilder(new BlockBuilderStatus(), 10);
//            for (int i = 0; i < 1000; i++) {
//                abstractType.writeLong(blockBuilder, (long) (34343+(Math.random()*100)));
//            }
//
//            Block build = blockBuilder.build();
//            Slice allocate = Slices.allocate(10000000);
//            SliceOutput output = allocate.getOutput();
//
//            blockBuilder.getEncoding().writeBlock(output, build);
//
//            assertTrue(build.getSizeInBytes() >= output.size());
//        }
    }

    @Test
    public void test2() throws IOException {
//        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BigintType.BIGINT, VarcharType.VARCHAR, VarcharType.VARCHAR));
//        List<Schema.Field> test = ImmutableList.of(
//                new Schema.Field("test", Schema.create(Schema.Type.LONG), null, null),
//                new Schema.Field("test1", Schema.create(Schema.Type.STRING), null, null),
//                new Schema.Field("test2", Schema.create(Schema.Type.STRING), null, null));
//        Schema schema = Schema.createRecord(test);
//
//        ByteArrayOutputStream output = new ByteArrayOutputStream();
//        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
//        GenericDatumWriter writer = new GenericDatumWriter(schema);
//        GenericData.Record record = new GenericData.Record(schema);
//        record.put("test", 10L);
//        record.put("test1", "tamam");
//        record.put("test2", "tamam2");
//        writer.write(record, encoder);
//        encoder.flush();

//        PageDatumReader pageDatumReader = new PageDatumReader(pageBuilder, schema);
//
//        byte[] bytes = output.toByteArray();
//
//        DecoderFactory decoderFactory = DecoderFactory.get();
//        BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(bytes, 0, bytes.length, null);
//
//        for (int i = 0; i < 10000000; i++) {
//            binaryDecoder = decoderFactory.binaryDecoder(bytes, 0, bytes.length, binaryDecoder);
//            pageDatumReader.read(null, binaryDecoder);
//        }
//
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        BlockEncodingManager serde = new BlockEncodingManager(new TypeRegistry());
//
//        PagesSerde.writePages(serde, new OutputStreamSliceOutput(out), pageBuilder.build());
//
//        byte[] serializedPage = out.toByteArray();
//
//        long l = System.currentTimeMillis();
//        Iterator<Page> pageIterator = PagesSerde
//                .readPages(serde, new InputStreamSliceInput(new ByteArrayInputStream(serializedPage)));
//        ImmutableList.copyOf(pageIterator);
//        System.out.println(System.currentTimeMillis()-l);
    }
}
