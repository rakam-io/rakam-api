import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.TestingConfigManager;
import org.rakam.analysis.InMemoryApiKeyService;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.*;
import org.rakam.config.ProjectConfig;
import org.rakam.util.AvroUtil;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableSet.of;
import static org.rakam.collection.FieldType.DOUBLE;
import static org.rakam.collection.FieldType.STRING;
import static org.testng.Assert.assertEquals;

public class TestCSVParser {
    @Test
    public void testName() throws Exception {
        CsvMapper mapper = new CsvMapper();

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        Metastore metastore = new InMemoryMetastore(new InMemoryApiKeyService(), new EventBus());
        mapper.registerModule(new SimpleModule().addDeserializer(EventList.class,
                new CsvEventDeserializer(metastore, new ProjectConfig(), new TestingConfigManager(), new SchemaChecker(metastore, build), build)));

        metastore.createProject("project");
        metastore.getOrCreateCollectionFields("project", "collection",
                of(new SchemaField("price", DOUBLE)));

        String csv = "Transaction_date,Product,Price\n" +
                "1/2/09 6:17,Product1,1200\n" +
                "1/2/09 4:53,Product2,1500\n";

        EventList actual = mapper.reader(EventList.class).with(ContextAttributes.getEmpty()
                .withSharedAttribute("project", "project")
                .withSharedAttribute("collection", "collection")
                .withSharedAttribute("apiKey", "apiKey")
        ).readValue(csv);

        List<SchemaField> collection = metastore.getCollection("project", "collection");

        assertEquals(ImmutableSet.copyOf(collection), ImmutableSet.of(
                new SchemaField("transaction_date", STRING),
                new SchemaField("product", STRING),
                new SchemaField("price", DOUBLE)));

        Schema avroSchema = AvroUtil.convertAvroSchema(collection);
        GenericData.Record record1 = new GenericData.Record(avroSchema);
        record1.put("transaction_date", "1/2/09 6:17");
        record1.put("product", "Product1");
        record1.put("price", 1200.0);

        GenericData.Record record2 = new GenericData.Record(avroSchema);
        record2.put("transaction_date", "1/2/09 4:53");
        record2.put("product", "Product2");
        record2.put("price", 1500.0);

        EventList eventList = new EventList(Event.EventContext.apiKey("apiKey"), "project", ImmutableList.of(
                new Event("project", "collection", null, ImmutableList.copyOf(collection), record1),
                new Event("project", "collection", null, ImmutableList.copyOf(collection), record2)));
        assertEquals(actual, eventList);
    }

}
