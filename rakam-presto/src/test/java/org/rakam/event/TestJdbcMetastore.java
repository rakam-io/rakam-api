package org.rakam.event;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import org.rakam.analysis.JDBCMetastore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.rakam.collection.FieldType.LONG;
import static org.rakam.collection.FieldType.STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJdbcMetastore extends TestingEnvironment {
    private JDBCMetastore metastore;

    public TestJdbcMetastore() throws Exception {
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
        JDBCPoolDataSource metastoreDataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);

        metastore = new JDBCMetastore(metastoreDataSource, prestoConfig,
                new EventBus(), new FieldDependencyBuilder().build());
        metastore.setup();
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
        metastore.destroy();
    }

    @Test
    public void testCreateProject() throws Exception {
        metastore.createProject("testing");
        assertEquals(metastore.getProjects(), ImmutableList.of("testing"));
    }

    @Test
    public void testCreateCollection() throws Exception {
        metastore.createProject("testing");

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        metastore.getOrCreateCollectionFields("testing", "test", schema);

        assertEquals(metastore.getCollection("testing", "test"), schema);
    }

    @Test
    public void testCreateFields() throws Exception {
        metastore.createProject("testing");

        metastore.getOrCreateCollectionFields("testing", "test", ImmutableSet.of());

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        metastore.getOrCreateCollectionFields("testing", "test", schema);

        assertEquals(metastore.getCollection("testing", "test"), schema);
    }

    @Test
    public void testCreateApiKeys() throws Exception {
        metastore.createProject("testing");

        Metastore.ProjectApiKeys testing = metastore.createApiKeys("testing");

        assertTrue(metastore.checkPermission("testing", Metastore.AccessKeyType.READ_KEY, testing.readKey));
        assertTrue(metastore.checkPermission("testing", Metastore.AccessKeyType.WRITE_KEY, testing.writeKey));
        assertTrue(metastore.checkPermission("testing", Metastore.AccessKeyType.MASTER_KEY, testing.masterKey));

        assertFalse(metastore.checkPermission("testing", Metastore.AccessKeyType.READ_KEY, "invalidKey"));
        assertFalse(metastore.checkPermission("testing", Metastore.AccessKeyType.WRITE_KEY, "invalidKey"));
        assertFalse(metastore.checkPermission("testing", Metastore.AccessKeyType.MASTER_KEY, "invalidKey"));
    }

    @Test
    public void testRevokeApiKeys() throws Exception {
        metastore.createProject("testing");

        Metastore.ProjectApiKeys testing = metastore.createApiKeys("testing");

        metastore.revokeApiKeys("testing", testing.id);

        assertFalse(metastore.checkPermission("testing", Metastore.AccessKeyType.READ_KEY, testing.readKey));
        assertFalse(metastore.checkPermission("testing", Metastore.AccessKeyType.WRITE_KEY, testing.writeKey));
        assertFalse(metastore.checkPermission("testing", Metastore.AccessKeyType.MASTER_KEY, testing.masterKey));
    }

    @Test
    public void testDeleteProject() throws Exception {
        metastore.createProject("testing");
        metastore.deleteProject("testing");

        assertFalse(metastore.getProjects().contains("testing"));
    }

    @Test
    public void testGetApiKeys() throws Exception {
        metastore.createProject("testing");

        Metastore.ProjectApiKeys testing = metastore.createApiKeys("testing");
        assertEquals(metastore.getApiKeys(new int[]{testing.id}), ImmutableList.of(testing));
    }

    @Test
    public void testCollectionMethods() throws Exception {
        metastore.createProject("testing");

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test1", STRING), new SchemaField("test2", STRING));
        metastore.getOrCreateCollectionFields("testing", "testcollection1", schema);

        metastore.getOrCreateCollectionFields("testing", "testcollection2", schema);

        assertEquals(metastore.getCollectionNames("testing"), ImmutableList.of("testcollection1", "testcollection2"));
        assertEquals(metastore.getCollections("testing"), ImmutableMap.of("testcollection1", schema, "testcollection2", schema));
    }

    @Test
    public void testCollectionFieldsOrdering() throws Exception {
        metastore.createProject("testing");

        ImmutableSet.Builder<SchemaField> builder = ImmutableSet.builder();

        for (FieldType fieldType : FieldType.values()) {
            builder.add(new SchemaField(fieldType.name(), fieldType));
        }

        metastore.getOrCreateCollectionFields("testing", "testcollection", builder.build());

        for (int i = 0; i < 100; i++) {
            assertEquals(metastore.getCollection("testing", "testcollection"), builder.build());
            metastore.clearCache();
        }
    }

    @Test
    public void testDuplicateFieldNames() throws Exception {
        metastore.createProject("testing");

        ImmutableSet.Builder<SchemaField> builder = ImmutableSet.builder();

        for (FieldType fieldType : FieldType.values()) {
            builder.add(new SchemaField(fieldType.name(), fieldType));
        }

        metastore.getOrCreateCollectionFields("testing", "testcollection",
                ImmutableSet.of(new SchemaField("test", STRING), new SchemaField("test", LONG)));
    }

    @Test
    public void testAllSchemaTypes() throws Exception {
        metastore.createProject("testing");

        ImmutableSet.Builder<SchemaField> builder = ImmutableSet.builder();

        for (FieldType fieldType : FieldType.values()) {
            builder.add(new SchemaField(fieldType.name(), fieldType));
        }

        metastore.getOrCreateCollectionFields("testing", "testcollection", builder.build());

        assertEquals(metastore.getCollection("testing", "testcollection"), builder.build());
    }

    /**
     * The schema change requests may be performed from any Rakam node in a cluster and they have to be consistent.
     **/
    @Test
    public void testConcurrentSchemaChanges() throws Exception {
        metastore.createProject("test");

        List<List<SchemaField>> collect = IntStream.range(0, 300).parallel().mapToObj(i ->
                metastore.getOrCreateCollectionFieldList("test", "test", ImmutableSet.of(new SchemaField("test" + i, FieldType.STRING))))
                .collect(Collectors.toList());

        List<SchemaField> allSchemas = collect.stream().sorted((o1, o2) -> o1.size() - o2.size()).findFirst().get();

        for (List<SchemaField> schemaFields : collect) {
            for (int i = 0; i < schemaFields.size(); i++) {
                assertEquals(schemaFields.get(i), allSchemas.get(i));
            }
        }
    }
}
