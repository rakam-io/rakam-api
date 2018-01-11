import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.generic.GenericArray;
import org.rakam.EventBuilder;
import org.rakam.TestingConfigManager;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.InMemoryApiKeyService;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.*;
import org.rakam.config.ProjectConfig;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestEventJsonParser {
    private ObjectMapper mapper;
    private ApiKeyService.ProjectApiKeys apiKeys;
    private EventBuilder eventBuilder;
    private InMemoryMetastore metastore;
    private JsonEventDeserializer eventDeserializer;
    private InMemoryApiKeyService apiKeyService;

    @BeforeSuite
    public void setUp()
            throws Exception {
        FieldDependencyBuilder.FieldDependency fieldDependency = new FieldDependencyBuilder().build();
        apiKeyService = new InMemoryApiKeyService();
        metastore = new InMemoryMetastore(apiKeyService);

        SchemaChecker schemaChecker = new SchemaChecker(metastore, fieldDependency);
        eventDeserializer = new JsonEventDeserializer(metastore, apiKeyService, new TestingConfigManager(), schemaChecker, new ProjectConfig(), fieldDependency);
        EventListDeserializer eventListDeserializer = new EventListDeserializer(apiKeyService, eventDeserializer);

        mapper = JsonHelper.getMapper();
        mapper.registerModule(new SimpleModule()
                .addDeserializer(Event.class, eventDeserializer)
                .addDeserializer(EventList.class, eventListDeserializer));
        eventBuilder = new EventBuilder("test", metastore);
    }

    @AfterMethod
    public void tearDownMethod()
            throws Exception {
        metastore.deleteProject("test");
        eventDeserializer.cleanCache();
        eventBuilder.cleanCache();
    }

    @BeforeMethod
    public void setupMethod()
            throws Exception {
        metastore.createProject("test");
        apiKeys = apiKeyService.createApiKeys("test");
    }

    @Test
    public void testSimple()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of()));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                .createEvent("test", ImmutableMap.of()).properties(), event.properties());
    }

    @Test
    public void testSimpleWithoutProject()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of()));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                .createEvent("test", ImmutableMap.of()).properties(), event.properties());
    }

    @Test
    public void testPrimitiveTypes()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> properties = ImmutableMap.of(
                "test", 1L,
                "test1", false,
                "test2", Instant.now(),
                "test3", "test",
                "test4", LocalDate.now());

        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", properties));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                .createEvent("test", properties).properties(), event.properties());

        assertEquals(ImmutableSet.copyOf(metastore.getCollection("test", "test")), ImmutableSet.of(
                new SchemaField("test", FieldType.DOUBLE),
                new SchemaField("_user", FieldType.STRING),
                new SchemaField("test1", FieldType.BOOLEAN),
                new SchemaField("test2", FieldType.TIMESTAMP),
                new SchemaField("test3", FieldType.STRING),
                new SchemaField("test4", FieldType.DATE)));
    }

    @Test
    public void testMapType()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> properties = ImmutableMap.of("test0", "test",
                "test1", ImmutableMap.of("a", 4.0, "b", 5.0, "c", 6.0, "d", 7.0),
                "test2", false);
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", properties));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                .createEvent("test", properties).properties(), event.properties());
    }

    @Test
    public void testArrayType()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> properties = ImmutableMap.of("test0", "test",
                "test1", ImmutableList.of("test", "test"),
                "test2", false);
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", properties));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                .createEvent("test", properties).properties(), event.properties());
    }

    public void testInvalidOrder()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "properties", ImmutableMap.of("test0", "test",
                        "test1", ImmutableList.of("test", "test"),
                        "test2", false),
                "api", api,
                "collection", "test"));

        mapper.readValue(bytes, Event.class);
    }

    @Test(expectedExceptions = RakamException.class)
    public void testInvalidField()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of("test0", "test",
                        "test1", ImmutableList.of("test", "test"),
                        "test2", false),
                "test", "test"
        ));

        Event event = mapper.readValue(bytes, Event.class);
        ;

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                .createEvent("test", ImmutableMap.of()).properties(), event.properties());
    }

    @Test()
    public void testInvalidArrayRecursiveType()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of("test0", "test",
                        "test1", ImmutableList.of("test", ImmutableMap.of("test", 2)),
                        "test2", false)));

        Event event = mapper.readValue(bytes, Event.class);
        GenericArray test1 = event.getAttribute("test1");
        assertEquals(test1.get(0), "test");
        assertEquals(test1.get(1), "{\"test\":2}");
    }

    @Test
    public void testInvalidMapRecursiveType()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of("test0", "test0",
                        "test1", ImmutableMap.of("test", ImmutableList.of("test4")),
                        "test2", false)));

        Event event = mapper.readValue(bytes, Event.class);
        Map test1 = event.getAttribute("test1");
        assertEquals(test1.get("test"), "[\"test4\"]");
    }

    @Test
    public void testInvalidArray()
            throws Exception {

        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of("test1", ImmutableList.of(true, 10))));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder.createEvent("test", ImmutableMap.of("test1", ImmutableList.of(true, true))).properties(),
                event.properties());
    }

    @Test
    public void testInvalidMap()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of("test1", ImmutableMap.of("test", 1, "test2", "test"))));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                        .createEvent("test", ImmutableMap.of("test1", ImmutableMap.of("test", 1.0, "test2", 0.0))).properties(),
                event.properties());
    }

    @Test
    public void testEmptyArray()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of("test", 1, "test2",
                        Arrays.asList(null, null), "test20", Arrays.asList(), "test3", true)));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                        .createEvent("test", ImmutableMap.of("test", 1.0, "test3", true)).properties(),
                event.properties());
    }

    @Test
    public void testEmptyMap()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "collection", "test",
                "api", api,
                "properties", ImmutableMap.of("test", 1, "test2",
                        new HashMap<String, String>() {
                            {
                                put("a", null);
                            }
                        }),
                "test20", ImmutableMap.of(), "test3", true));

        Event event = mapper.readValue(bytes, Event.class);

        assertEquals("test", event.project());
        assertEquals("test", event.collection());
        assertEquals(api, event.api());
        assertEquals(eventBuilder
                        .createEvent("test", ImmutableMap.of("test", 1.0, "test3", true)).properties(),
                event.properties());
    }

    @Test
    public void testBatch()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> props = ImmutableMap.of(
                "test0", "test",
                "test1", ImmutableList.of("test"),
                "test2", false);
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "api", api,
                "events", ImmutableList.of(
                        ImmutableMap.of("collection", "test", "properties", props),
                        ImmutableMap.of("collection", "test", "properties", props))));

        EventList events = mapper.readValue(bytes, EventList.class);

        assertEquals("test", events.project);
        assertEquals(api, events.api);

        for (Event event : events.events) {
            assertEquals("test", event.collection());

            assertEquals(eventBuilder.createEvent("test", props).properties(), event.properties());
        }
    }

    @Test
    public void testBatchWithoutProject()
            throws Exception {
        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> props = ImmutableMap.of(
                "test0", "test",
                "test1", ImmutableList.of("test"),
                "test2", false);
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "api", api,
                "events", ImmutableList.of(
                        ImmutableMap.of("collection", "test", "properties", props),
                        ImmutableMap.of("collection", "test", "properties", props))));

        EventList events = mapper.readValue(bytes, EventList.class);

        assertEquals("test", events.project);
        assertEquals(api, events.api);

        for (Event event : events.events) {
            assertEquals("test", event.collection());

            assertEquals(eventBuilder.createEvent("test", props).properties(), event.properties());
        }
    }

    @Test
    public void testObjectSentToScalarValue()
            throws Exception {
        metastore.getOrCreateCollectionFields("test", "test",
                ImmutableSet.of(new SchemaField("test", FieldType.STRING)));

        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> props = ImmutableMap.of(
                "test", ImmutableList.of("test"));
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "api", api,
                "collection", "test",
                "properties", props));

        Event events = mapper.readValue(bytes, Event.class);
        assertEquals(events.properties().get("test"), "[\"test\"]");
    }

    //    @Test(expectedExceptions = JsonMappingException.class, expectedExceptionsMessageRegExp = "Cannot cast object to INTEGER for 'test' field.*")
    @Test()
    public void testObjectSentToInvalidScalarValue()
            throws Exception {
        metastore.getOrCreateCollectionFields("test", "test",
                ImmutableSet.of(new SchemaField("test", FieldType.INTEGER)));

        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> props = ImmutableMap.of(
                "test", ImmutableList.of("test"));
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "api", api,
                "collection", "test",
                "properties", props));

        Event event = mapper.readValue(bytes, Event.class);
        assertNull(event.getAttribute("test"));
    }

    @Test(expectedExceptions = JsonMappingException.class, expectedExceptionsMessageRegExp = "Scalar value 'test' cannot be cast to ARRAY_BOOLEAN type for 'test' field.*")
    public void testScalarSentToObjectValue()
            throws Exception {
        metastore.getOrCreateCollectionFields("test", "test",
                ImmutableSet.of(new SchemaField("test", FieldType.ARRAY_BOOLEAN)));

        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());
        ImmutableMap<String, Object> props = ImmutableMap.of(
                "test", "test");
        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "api", api,
                "collection", "test",
                "properties", props));

        mapper.readValue(bytes, Event.class);
    }

    @Test
    public void testNullSentToObjectValue()
            throws Exception {
        metastore.getOrCreateCollectionFields("test", "test",
                ImmutableSet.of(new SchemaField("test", FieldType.ARRAY_BOOLEAN)));

        Event.EventContext api = Event.EventContext.apiKey(apiKeys.writeKey());

        HashMap<String, Object> props = new HashMap<>();
        props.put("test", null);

        byte[] bytes = mapper.writeValueAsBytes(ImmutableMap.of(
                "api", api,
                "collection", "test",
                "properties", props));

        Event event = mapper.readValue(bytes, Event.class);
        assertNull(event.properties().get("test"));
    }
}
