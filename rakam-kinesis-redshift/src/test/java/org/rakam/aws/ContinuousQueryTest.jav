package org.rakam.aws;

import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.IMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;
import org.rakam.analysis.ContinuousQueryAnalyzer;
import org.rakam.analysis.ProjectNotExistsException;
import org.rakam.analysis.stream.QueryAnalyzer;
import org.rakam.analysis.stream.RakamContinuousQueryService;
import org.rakam.analysis.stream.processor.Processor;
import org.rakam.analysis.worker.KinesisMessageEventTransformer.CollectionPageBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryResult;
import org.rakam.util.Tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static java.util.Collections.unmodifiableList;
import static org.junit.Assert.assertEquals;

/**
 * Created by buremba <Burak Emre Kabakcı> on 07/07/15 05:46.
 */
public class ContinuousQueryTest {
    @Test
    public void testTest() throws IOException, ProjectNotExistsException {
        IMap<Tuple<String, String>, Processor> continuousQueryMap = new RedshiftModule.HazelcastInstanceProvider(new QueryAnalyzer()).get().getMap("continuousQueryies");
        ContinuousQueryAnalyzer continuousQueryAnalyzer = new ContinuousQueryAnalyzer(continuousQueryMap);

        InMemoryMetastore metastore = new InMemoryMetastore();

        metastore.createProject("test");
        metastore.createOrGetCollectionField("test", "pageView",
                of(new SchemaField("test", FieldType.STRING, false)));

        InMemoryQueryMetadataStore database = new InMemoryQueryMetadataStore();
        ContinuousQuery report = new ContinuousQuery("test", "test", "test", "select test, approx_distinct(test) unique, count(test) count from stream group by 1", of(), ImmutableMap.of());

        RakamContinuousQueryService service = new RakamContinuousQueryService(
                database,
                metastore,
                continuousQueryAnalyzer,
                continuousQueryMap);
        service.create(report);

        Schema schema = Schema.createRecord(of(new Schema.Field("test", Schema.create(Schema.Type.STRING), null, null)));

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(generateEventData(schema, 3), null);

        CollectionPageBuilder pageBuilder = new CollectionPageBuilder(schema, of(VarcharType.VARCHAR));
        for (int i = 0; i < 3; i++) {
            pageBuilder.read(decoder);
        }

        service.updateReport(report, of(pageBuilder.buildPage().getPage()));

        QueryResult actualResult = service.executeQuery("test",  "select a.unique from test a join test b on (a.test = b.test)");
        List<SchemaField> columns = of(
                new SchemaField("test", FieldType.STRING, false),
                new SchemaField("unique", FieldType.LONG, false),
                new SchemaField("count", FieldType.LONG, false));
        List<List<Object>> result = unmodifiableList(of(unmodifiableList(of("test", 1L, 3L))));
        QueryResult expectedResult = new QueryResult(columns, result);

        assertEquals(actualResult, expectedResult);
    }


    private byte[] generateEventData(Schema schema, int count) throws IOException {
        GenericData.Record test;
        GenericDatumWriter genericDatumWriter = new GenericDatumWriter(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        for (int i = 0; i < count; i++) {
            test = new GenericData.Record(schema);
            test.put(0, "test");
            encoder.writeString("test");
            encoder.writeString("test");
            genericDatumWriter.write(test, encoder);
        }

        encoder.flush();

        return outputStream.toByteArray();
    }


}
