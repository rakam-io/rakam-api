import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.collection.Event;
import org.rakam.collection.event.EventCollectionHttpService;
import org.rakam.collection.event.EventDeserializer;
import org.rakam.collection.event.EventListDeserializer;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

public class TestEventJsonParser {
    private ObjectMapper mapper;
    private Metastore.ProjectApiKeys apiKeys;

    @BeforeSuite
    public void setup() throws Exception {
        FieldDependencyBuilder.FieldDependency fieldDependency = new FieldDependencyBuilder().build();
        InMemoryMetastore metastore = new InMemoryMetastore();
        metastore.createProject("test");
        apiKeys = metastore.createApiKeys("test");

        EventDeserializer eventDeserializer = new EventDeserializer(metastore, fieldDependency);
        EventListDeserializer eventListDeserializer = new EventListDeserializer(metastore, fieldDependency);

        mapper = new ObjectMapper().registerModule(new SimpleModule()
                .addDeserializer(Event.class, eventDeserializer)
                .addDeserializer(EventCollectionHttpService.EventList.class, eventListDeserializer));

    }

    @Test
    public void testName() throws Exception {
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "project", "test",
                "collection", "test",
                "api", new Event.EventContext(apiKeys.writeKey, "1.0", null, null),
                "properties", ImmutableMap.of()));

        mapper.readValue(bytes, Event.class);
    }
}
