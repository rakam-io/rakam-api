package org.rakam.collection;

import com.google.common.collect.ImmutableSet;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.rakam.collection.FieldType.*;
import static org.testng.Assert.*;

public abstract class TestMetastore {
    private static final String PROJECT_NAME = TestMetastore.class.getName().replace(".", "_").toLowerCase();

    public abstract AbstractMetastore getMetastore();

    @AfterMethod
    public void tearDownMethod() throws Exception {
        getMetastore().deleteProject(PROJECT_NAME);
    }

    @Test
    public void testCreateProject() throws Exception {
        getMetastore().createProject(PROJECT_NAME);
        assertTrue(getMetastore().getProjects().contains(PROJECT_NAME));
    }

    @Test
    public void testCreateCollection() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", schema);

        assertTrue(getMetastore().getCollection(PROJECT_NAME, "test").containsAll(schema));
    }

    @Test
    public void testCreateFields() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", ImmutableSet.of());

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", schema);

        assertTrue(getMetastore().getCollection(PROJECT_NAME, "test").containsAll(schema));
    }

    @Test
    public void testDeleteProject() throws Exception {
        getMetastore().createProject(PROJECT_NAME);
        getMetastore().deleteProject(PROJECT_NAME);

        assertFalse(getMetastore().getProjects().contains(PROJECT_NAME));
    }

    @Test
    public void testCollectionMethods() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test1", STRING), new SchemaField("test2", STRING));
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "testcollection1", schema);
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "testcollection2", schema);

        assertEquals(ImmutableSet.of("testcollection1", "testcollection2"), ImmutableSet.copyOf(getMetastore().getCollectionNames(PROJECT_NAME)));

        Map<String, List<SchemaField>> testing = getMetastore().getCollections(PROJECT_NAME);
        assertEquals(testing.size(), 2);
        assertTrue(ImmutableSet.copyOf(testing.get("testcollection1")).containsAll(schema));
        assertTrue(ImmutableSet.copyOf(testing.get("testcollection2")).containsAll(schema));
    }

    @Test
    public void testCollectionFieldsOrdering() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        ImmutableSet.Builder<SchemaField> builder = ImmutableSet.builder();

        for (FieldType fieldType : FieldType.values()) {
            builder.add(new SchemaField(fieldType.name(), fieldType));
        }

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "testcollection", builder.build());

        for (int i = 0; i < 100; i++) {
            assertTrue(getMetastore().getCollection(PROJECT_NAME, "testcollection").containsAll(builder.build()));
        }
    }

    @Test
    public void testDuplicateFields() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        ImmutableSet.Builder<SchemaField> builder = ImmutableSet.builder();

        for (FieldType fieldType : FieldType.values()) {
            builder.add(new SchemaField(fieldType.name(), fieldType));
        }

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "testcollection",
                ImmutableSet.of(new SchemaField("test", LONG)));

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "testcollection",
                ImmutableSet.of(new SchemaField("test", LONG)));

        assertTrue(ImmutableSet.copyOf(getMetastore().getCollection(PROJECT_NAME, "testcollection")).containsAll(
                ImmutableSet.of(new SchemaField("test", LONG), new SchemaField("test", LONG))));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testInvalidDuplicateFieldNames() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "testcollection",
                ImmutableSet.of(new SchemaField("test", STRING), new SchemaField("test", LONG)));
    }

    @Test
    public void testAllSchemaTypes() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        ImmutableSet.Builder<SchemaField> builder = ImmutableSet.builder();

        for (FieldType fieldType : FieldType.values()) {
            builder.add(new SchemaField(fieldType.name(), fieldType));
        }

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "testcollection", builder.build());

        assertTrue(getMetastore().getCollection(PROJECT_NAME, "testcollection").containsAll(builder.build()));
    }

    /**
     * The schema change requests may be performed from any Rakam node in a cluster and they have to be consistent.
     **/
    @Test
    public void testConcurrentSchemaChanges() throws Exception {
        getMetastore().createProject("test");

        List<List<SchemaField>> collect = IntStream.range(0, 10).parallel().mapToObj(i ->
                getMetastore().getOrCreateCollectionFields("test", "test", ImmutableSet.of(new SchemaField("test" + i, STRING))))
                .collect(Collectors.toList());

        Set<SchemaField> allSchemas = ImmutableSet.copyOf(collect.stream().sorted((o1, o2) -> o2.size() - o1.size()).findFirst().get());

        for (List<SchemaField> schemaFields : collect) {
            for (int i = 0; i < schemaFields.size(); i++) {
                assertTrue(allSchemas.contains(schemaFields.get(i)),
                        String.format("%s not in %s", schemaFields.get(i), allSchemas));
            }
        }
    }

    @Test
    public void testGetAttributes() {
        getMetastore().createProject(PROJECT_NAME);

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", ImmutableSet.of(new SchemaField("_time", TIMESTAMP), new SchemaField("test", STRING)));

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", schema);

        assertEquals(getMetastore().getAttributes(PROJECT_NAME, "test", "test", Optional.empty(), Optional.empty(), Optional.empty()).join().size(), 0);
    }

    @Test
    public void testGetAttributesWithTime() {
        getMetastore().createProject(PROJECT_NAME);

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", ImmutableSet.of(new SchemaField("_time", TIMESTAMP), new SchemaField("test", STRING)));

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", schema);

        assertEquals(getMetastore().getAttributes(PROJECT_NAME, "test", "test", Optional.of(LocalDate.parse("2017-01-01")), Optional.of(LocalDate.parse("2017-02-01")), Optional.empty()).join().size(), 0);
    }

    @Test
    public void testGetAttributesWithTimeAndFilter() {
        getMetastore().createProject(PROJECT_NAME);

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", ImmutableSet.of(new SchemaField("_time", TIMESTAMP), new SchemaField("test", STRING)));

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", schema);

        assertEquals(getMetastore().getAttributes(PROJECT_NAME, "test", "test", Optional.of(LocalDate.parse("2017-01-01")), Optional.of(LocalDate.parse("2017-02-01")), Optional.of("T")).join().size(), 0);
    }

    @Test
    public void testGetAttributesWithFilter() {
        getMetastore().createProject(PROJECT_NAME);

        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", ImmutableSet.of(new SchemaField("_time", TIMESTAMP), new SchemaField("test", STRING)));

        ImmutableSet<SchemaField> schema = ImmutableSet.of(new SchemaField("test", STRING));
        getMetastore().getOrCreateCollectionFields(PROJECT_NAME, "test", schema);

        assertEquals(getMetastore().getAttributes(PROJECT_NAME, "test", "test", Optional.empty(), Optional.empty(), Optional.of("T")).join().size(), 0);
    }
}
