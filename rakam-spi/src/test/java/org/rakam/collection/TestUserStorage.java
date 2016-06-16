package org.rakam.collection;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.User;
import org.rakam.util.JsonHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

public abstract class TestUserStorage
{
    private final String PROJECT_NAME = this.getClass().getSimpleName().toLowerCase();

    private ObjectNode sampleProperties = JsonHelper.jsonObject()
            .put("test", 1.0)
            .put("test1 Naber Abi", "value")
            .put("test4 Şamil", true)
            .put("created_at", 100)
            .put("test5", 1.5);
    private ObjectNode samplePropertiesExpected = JsonHelper.jsonObject()
            .put("test", 1.0)
            .put("test1 naber abi", "value")
            .put("test4 şamil", true)
            .put("created_at", Instant.ofEpochMilli(100).toString())
            .put("test5", 1.5);

    @BeforeSuite
    public void setUp()
            throws Exception
    {
        getMetastore().createProject(PROJECT_NAME);
    }

    @AfterSuite
    public void setDown()
            throws Exception
    {
        getMetastore().deleteProject(PROJECT_NAME);
    }

    @AfterMethod
    public void deleteProject()
            throws Exception
    {
        getUserService().dropProject(PROJECT_NAME);
    }

    @Test
    public void testCreate()
            throws Exception
    {
        AbstractUserService userService = getUserService();

        userService.create(PROJECT_NAME, 1L, sampleProperties);

        User test = userService.getUser(PROJECT_NAME, 1L).join();
        assertEquals(test.id, 1L);
        assertEquals((Object) test.properties, samplePropertiesExpected);
    }

    @Test
    public void testCastingSetProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 2, sampleProperties);

        userService.setUserProperties(PROJECT_NAME, 2, JsonHelper.jsonObject()
                .put("test", "2")
                .put("test1 Naber abi", 324)
                .put("test4 şamil", "true")
                .put("created_at", "test")
                .put("test5", "2.5"));

        User test = userService.getUser(PROJECT_NAME, 2).join();
        assertEquals(test.id, 2);
        assertEquals((Object) test.properties, JsonHelper.jsonObject()
                .put("test", 2.0)
                .put("test1 naber abi", "324")
                .put("test4 şamil", true)
                .put("created_at", Instant.ofEpochMilli(100).toString())
                .put("test5", 2.5));
    }

    @Test
    public void testSetProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 3, sampleProperties);

        User test = userService.getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        assertEquals((Object) test.properties, samplePropertiesExpected);
    }

    @Test
    public void testSetNullProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();

        userService.setUserProperties(PROJECT_NAME, 3, JsonHelper.jsonObject()
                .put("test", (String) null)
                .put("test1", (String) null));

        User test = userService.getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        assertNotNull(test.properties.get("created_at").asText());
    }

    @Test
    public void testSetSomeOfNullProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();

        userService.setUserProperties(PROJECT_NAME, 3, JsonHelper.jsonObject()
                .put("test10", "val")
                .put("created_at", 100.0)
                .put("test", (String) null)
                .put("test1", (String) null)
        );

        User test = userService.getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        assertEquals((Object) test.properties, JsonHelper.jsonObject()
                .put("test10", "val")
                .put("created_at", Instant.ofEpochMilli(100).toString()));
    }

    @Test
    public void testConcurrentSetProperties()
            throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        Set<String> objects = new ConcurrentSkipListSet<>();

        getUserService().setUserProperties(PROJECT_NAME, 3, JsonHelper.jsonObject().put("created_at", 100));
        CountDownLatch countDownLatch = new CountDownLatch(1000);

        for (int x = 0; x < 1000; x++) {
            executorService.submit(() -> {
                AbstractUserService userService = getUserService();
                ObjectNode builder = JsonHelper.jsonObject();
                for (int i = 0; i < 4; i++) {
                    String key = "test" + ((int) (Math.random() * 100));
                    objects.add(key);
                    builder.put(key, 10L);
                }
                userService.setUserProperties(PROJECT_NAME, 3, builder);
                countDownLatch.countDown();
            });
        }

        countDownLatch.await(1, TimeUnit.MINUTES);

        User test = getUserService().getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        ObjectNode builder = JsonHelper.jsonObject();
        builder.put("created_at", Instant.ofEpochMilli(100).toString());
        for (String object : objects) {
            builder.put(object, 10.0);
        }
        assertEquals((Object) test.properties, builder);
    }

    @Test
    public void testChangeSchemaSetProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 4, sampleProperties);

        ObjectNode newProperties = JsonHelper.jsonObject()
                .put("test100", 1.0)
                .put("test200", "value")
                .put("test400", true);
        userService.setUserProperties(PROJECT_NAME, 4, newProperties);

        User test = userService.getUser(PROJECT_NAME, 4).join();
        assertEquals(test.id, 4);

        ObjectNode builder = JsonHelper.jsonObject();
        builder.setAll(samplePropertiesExpected);
        builder.setAll(newProperties);

        assertEquals((Object) test.properties, builder);
    }

    @Test
    public void testSetOncePropertiesFirstSet()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserPropertiesOnce(PROJECT_NAME, 5, sampleProperties);

        User test = userService.getUser(PROJECT_NAME, 5).join();
        assertEquals(test.id, 5);
        assertEquals((Object) test.properties, samplePropertiesExpected);
    }

    @Test
    public void testSetOncePropertiesLatterSet()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 6, sampleProperties);

        userService.setUserPropertiesOnce(PROJECT_NAME, 6, JsonHelper.jsonObject()
                .put("test", 2)
                .put("test1 Naber Abi", "value1")
                .put("test4 Şamil", false)
                .put("created_at", Instant.now().toEpochMilli())
                .put("test5", 2.5));

        User test = userService.getUser(PROJECT_NAME, 6).join();
        assertEquals((Object) test.properties, samplePropertiesExpected);
    }

    @Test
    public void testUnsetSetProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 7, sampleProperties);

        userService.unsetProperties(PROJECT_NAME, 7, ImmutableList.of(
                "test",
                "test1 Naber Abi",
                "test4 Şamil"));

        User test = userService.getUser(PROJECT_NAME, 7).join();
        assertEquals(test.id, 7);
        assertEquals((Object) test.properties, JsonHelper.jsonObject()
                .put("created_at", Instant.ofEpochMilli(100).toString())
                .put("test5", 1.5));
    }

    @Test
    public void testIncrementProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.incrementProperty(PROJECT_NAME, 8, "test", 10);

        assertEquals(userService.getUser(PROJECT_NAME, 8).join().properties.get("test").asDouble(), 10.0);

        userService.incrementProperty(PROJECT_NAME, 8, "test", 10);
        assertEquals(userService.getUser(PROJECT_NAME, 8).join().properties.get("test").asDouble(), 20.0);
    }

    public abstract AbstractUserService getUserService();

    public abstract Metastore getMetastore();
}
