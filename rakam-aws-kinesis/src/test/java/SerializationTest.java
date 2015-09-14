import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 22:51.
 */
public class SerializationTest {
    @Test
    public void test() throws IOException {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BigintType.BIGINT, VarcharType.VARCHAR, VarcharType.VARCHAR));
        List<Schema.Field> test = ImmutableList.of(
                new Schema.Field("test", Schema.create(Schema.Type.LONG), null, null),
                new Schema.Field("test1", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("test2", Schema.create(Schema.Type.STRING), null, null));
        Schema schema = Schema.createRecord(test);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        GenericDatumWriter writer = new GenericDatumWriter(schema);
        GenericData.Record record = new GenericData.Record(schema);
        record.put("test", 10L);
        record.put("test1", "tamam");
        record.put("test2", "tamam2");
        writer.write(record, encoder);
        encoder.flush();

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

        ImmutableList<AbstractType> of = ImmutableList.of(DoubleType.DOUBLE);
        for (AbstractType abstractType : of) {
            BlockBuilder blockBuilder = abstractType.createBlockBuilder(new BlockBuilderStatus(), 10);
            for (int i = 0; i < 1000; i++) {
                abstractType.writeLong(blockBuilder, (long) (34343+(Math.random()*100)));
            }

            Block build = blockBuilder.build();
            Slice allocate = Slices.allocate(10000000);
            SliceOutput output = allocate.getOutput();

            blockBuilder.getEncoding().writeBlock(output, build);

            assertTrue(build.getSizeInBytes() >= output.size());
        }
    }

    @Test
    public void test2() throws IOException {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BigintType.BIGINT, VarcharType.VARCHAR, VarcharType.VARCHAR));
        List<Schema.Field> test = ImmutableList.of(
                new Schema.Field("test", Schema.create(Schema.Type.LONG), null, null),
                new Schema.Field("test1", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("test2", Schema.create(Schema.Type.STRING), null, null));
        Schema schema = Schema.createRecord(test);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        GenericDatumWriter writer = new GenericDatumWriter(schema);
        GenericData.Record record = new GenericData.Record(schema);
        record.put("test", 10L);
        record.put("test1", "tamam");
        record.put("test2", "tamam2");
        writer.write(record, encoder);
        encoder.flush();

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

    @Test
    public void testName() throws Exception {
        AtomicBoolean test = new AtomicBoolean();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    test.set(true);
                }
            }
        });

        thread.start();

        synchronized (thread) {
            thread.wait();
        }

        test.set(false);

        Thread.sleep(1000);

        System.out.println(test.get());

    }
}
