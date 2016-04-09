import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.InMemoryApiKeyService;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.CsvEventDeserializer;
import org.rakam.collection.Event;
import org.rakam.collection.EventList;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.SchemaField;
import org.rakam.util.AvroUtil;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static org.rakam.collection.FieldType.DOUBLE;
import static org.rakam.collection.FieldType.STRING;
import static org.testng.Assert.assertEquals;

public class TestCSVParser {
    @Test
    public void testName() throws Exception {
        CsvMapper mapper = new CsvMapper();

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        Metastore metastore = new InMemoryMetastore(build, new InMemoryApiKeyService(), new EventBus());
        mapper.registerModule(new SimpleModule().addDeserializer(EventList.class, new CsvEventDeserializer(metastore, build)));

        metastore.createProject("project");
        metastore.getOrCreateCollectionFieldList("project", "collection",
                of(new SchemaField("price", DOUBLE)));

        String csv = "Transaction_date,Product,Price\n" +
                "1/2/09 6:17,Product1,1200\n" +
                "1/2/09 4:53,Product2,1500\n";

        EventList actual = mapper.reader(EventList.class).with(ContextAttributes.getEmpty()
                        .withSharedAttribute("project", "project")
                        .withSharedAttribute("collection", "collection")
                        .withSharedAttribute("api_key", "api_key")
        ).readValue(csv);

        Event.EventContext api = new Event.EventContext("api_key", null, null, null);

        List<SchemaField> collection = metastore.getCollection("project", "collection");
        Set<SchemaField> schema = ImmutableSet.of(
                new SchemaField("transaction_date", STRING),
                new SchemaField("product", STRING),
                new SchemaField("price", DOUBLE));

        assertEquals(ImmutableSet.copyOf(collection), schema);

        Schema avroSchema = AvroUtil.convertAvroSchema(collection);
        GenericData.Record record1 = new GenericData.Record(avroSchema);
        record1.put("transaction_date", "1/2/09 6:17");
        record1.put("product", "Product1");
        record1.put("price", 1200.0);

        GenericData.Record record2 = new GenericData.Record(avroSchema);
        record2.put("transaction_date", "1/2/09 4:53");
        record2.put("product", "Product2");
        record2.put("price", 1500.0);

        EventList eventList = new EventList(api, "project",
                ImmutableList.of(new Event("project", "collection", null, record1),
                        new Event("project", "collection", null, record2)));
        assertEquals(actual, eventList);
    }

}
