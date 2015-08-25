package org.rakam.aws;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Test;
import org.rakam.analysis.stream.RakamContinuousQueryService;
import org.rakam.analysis.ProjectNotExistsException;
import org.rakam.analysis.stream.StreamingEmitter;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryResult;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;
import static java.util.Collections.unmodifiableList;

/**
 * Created by buremba <Burak Emre Kabakcı> on 07/07/15 05:46.
 */
public class ContinuousQueryTest {
    @Test
    public void testTest() throws IOException {
        RakamContinuousQueryService service = new RakamContinuousQueryService(new MyQueryMetadataStore(), new MyMetastore(), new HazelcastInstanceProvider());
        StreamingEmitter streamingEmitter = new StreamingEmitter(service);

        ArrayList<Event> objects;
        GenericData.Record test;

        objects = new ArrayList<>();
        test = new GenericData.Record(Schema.createRecord(of(new Schema.Field("test", Schema.create(Schema.Type.STRING), null, null))));
        test.put(0, "test");
        objects.add(Event.create("test", "test", test));

        test = new GenericData.Record(Schema.createRecord(of(new Schema.Field("test", Schema.create(Schema.Type.STRING), null, null))));
        test.put(0, "test");
        objects.add(Event.create("test", "test", test));

        test = new GenericData.Record(Schema.createRecord(of(new Schema.Field("test", Schema.create(Schema.Type.STRING), null, null))));
        test.put(0, "test");
        objects.add(Event.create("test", "test", test));

        streamingEmitter.emit(objects);
        streamingEmitter.emit(objects);

        QueryResult result = service.executeQuery("test", "test", "select * from test");
        QueryResult expectedResult = new QueryResult(of(new SchemaField("", FieldType.LONG, false)), unmodifiableList(of(unmodifiableList(of(6)))));
//        assertEquals(result, expectedResult);
    }


    private static class MyMetastore implements Metastore {
        @Override
        public Map<String, List<String>> getAllCollections() {
            return null;
        }

        @Override
        public Map<String, List<SchemaField>> getCollections(String project) {
            return ImmutableMap.of("pageView", of(new SchemaField("test", FieldType.STRING, false)));
        }

        @Override
        public void createProject(String project) {

        }

        @Override
        public List<String> getProjects() {
            return null;
        }

        @Override
        public List<SchemaField> getCollection(String project, String collection) {
            return null;
        }

        @Override
        public List<SchemaField> createOrGetCollectionField(String project, String collection, List<SchemaField> fields) throws ProjectNotExistsException {
            return null;
        }
    }

    private static class MyQueryMetadataStore implements QueryMetadataStore {
        @Override
        public void createMaterializedView(MaterializedView materializedView) {

        }

        @Override
        public void deleteMaterializedView(String project, String name) {

        }

        @Override
        public MaterializedView getMaterializedView(String project, String name) {
            return null;
        }

        @Override
        public List<MaterializedView> getMaterializedViews(String project) {
            return of();
        }

        @Override
        public List<MaterializedView> getAllMaterializedViews() {
            return null;
        }

        @Override
        public void updateMaterializedView(String project, String name, Instant last_update) {

        }

        @Override
        public void createContinuousQuery(ContinuousQuery report) {

        }

        @Override
        public void deleteContinuousQuery(String project, String name) {

        }

        @Override
        public List<ContinuousQuery> getContinuousQueries(String project) {
            return of(new ContinuousQuery(project, "test", "test", "select test, approx_distinct(test), count(test) count0 from stream group by 1", of(), ImmutableMap.of()));
        }

        @Override
        public ContinuousQuery getContinuousQuery(String project, String name) {
            return null;
        }

        @Override
        public List<ContinuousQuery> getAllContinuousQueries() {
            return null;
        }
    }
}
