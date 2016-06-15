package org.rakam.collection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.User;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestUserStorage
{
    private final String PROJECT_NAME = this.getClass().getSimpleName().toLowerCase();

    private ImmutableMap<String, Object> sampleProperties = ImmutableMap.of(
            "test", 1L,
            "test1 Naber Abi", "value",
            "test4 Şamil", true,
            "created_at", 100,
            "test5", 1.5);
    private ImmutableMap<String, Object> samplePropertiesExpected = ImmutableMap.of(
            "test", 1L,
            "test1 naber abi", "value",
            "test4 şamil", true,
            "created_at", Instant.ofEpochMilli(100),
            "test5", 1.5);

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
        assertEquals(test.properties, samplePropertiesExpected);
    }

    @Test
    public void testCastingSetProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 2, sampleProperties);

        userService.setUserProperties(PROJECT_NAME, 2, ImmutableMap.of(
                "test", "2",
                "test1 Naber abi", 324,
                "test4 şamil", "true",
                "created_at", PROJECT_NAME,
                "test5", "2.5"));

        User test = userService.getUser(PROJECT_NAME, 2).join();
        assertEquals(test.id, 2);
        assertEquals(test.properties, ImmutableMap.of(
                "test", 2L,
                "test1 naber abi", "324",
                "test4 şamil", true,
                "created_at", Instant.ofEpochMilli(100),
                "test5", 2.5));
    }

    @Test
    public void testSetProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 3, sampleProperties);

        User test = userService.getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        assertEquals(test.properties, samplePropertiesExpected);
    }

    @Test
    public void testSetNullProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        HashMap<String, Object> map = new HashMap<>();
        map.put("test", null);
        map.put("test1", null);

        userService.setUserProperties(PROJECT_NAME, 3, map);

        User test = userService.getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        assertTrue(test.properties.get("created_at") instanceof Instant);
    }

    @Test
    public void testSetSomeOfNullProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        HashMap<String, Object> map = new HashMap<>();
        map.put("test10", "val");
        map.put("created_at", 100);
        map.put("test", null);
        map.put("test1", null);

        userService.setUserProperties(PROJECT_NAME, 3, map);

        User test = userService.getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        assertEquals(test.properties, ImmutableMap.of(
                "test10", "val",
                "created_at", Instant.ofEpochMilli(100)));
    }

    @Test
    public void testConcurrentSetProperties()
            throws Exception
    {

        ExecutorService executorService = Executors.newFixedThreadPool(8);

        Set<String> objects = new ConcurrentSkipListSet<>();

        getUserService().setUserProperties(PROJECT_NAME, 3, ImmutableMap.of("created_at", 100));
        CountDownLatch countDownLatch = new CountDownLatch(1000);

        for (int x = 0; x < 1000; x++) {
            executorService.submit(() -> {
                AbstractUserService userService = getUserService();
                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                for (int i = 0; i < 4; i++) {
                    String key = "test" + ((int) (Math.random() * 100));
                    objects.add(key);
                    builder.put(key, 10L);
                }
                userService.setUserProperties(PROJECT_NAME, 3, builder.build());
                countDownLatch.countDown();
            });
        }

        countDownLatch.await(1, TimeUnit.MINUTES);

        User test = getUserService().getUser(PROJECT_NAME, 3).join();
        assertEquals(test.id, 3);
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("created_at", Instant.ofEpochMilli(100));
        for (String object : objects) {
            builder.put(object, 10L);
        }
        assertEquals(test.properties, builder.build());
    }

    @Test
    public void testChangeSchemaSetProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 4, sampleProperties);

        ImmutableMap<String, Object> newProperties = ImmutableMap.of(
                "test100", 1L,
                "test200", "value",
                "test400", true);
        userService.setUserProperties(PROJECT_NAME, 4, newProperties);

        User test = userService.getUser(PROJECT_NAME, 4).join();
        assertEquals(test.id, 4);

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.putAll(samplePropertiesExpected);
        builder.putAll(newProperties);

        assertEquals(test.properties, builder.build());
    }

    @Test
    public void testSetOncePropertiesFirstSet()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserPropertiesOnce(PROJECT_NAME, 5, sampleProperties);

        User test = userService.getUser(PROJECT_NAME, 5).join();
        assertEquals(test.id, 5);
        assertEquals(test.properties, samplePropertiesExpected);
    }

    @Test
    public void testSetOncePropertiesLatterSet()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.setUserProperties(PROJECT_NAME, 6, sampleProperties);

        userService.setUserPropertiesOnce(PROJECT_NAME, 6, ImmutableMap.of(
                "test", 2,
                "test1 Naber Abi", "value1",
                "test4 Şamil", false,
                "created_at", Instant.now().toEpochMilli(),
                "test5", 2.5));

        User test = userService.getUser(PROJECT_NAME, 6).join();
        assertEquals(test.properties, samplePropertiesExpected);
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
        assertEquals(test.properties, ImmutableMap.of(
                "created_at", Instant.ofEpochMilli(100),
                "test5", 1.5));
    }

    @Test
    public void testIncrementProperties()
            throws Exception
    {
        AbstractUserService userService = getUserService();
        userService.incrementProperty(PROJECT_NAME, 8, "test", 10);

        assertEquals(userService.getUser(PROJECT_NAME, 8).join().properties.get("test"), 10.0);

        userService.incrementProperty(PROJECT_NAME, 8, "test", 10);
        assertEquals(userService.getUser(PROJECT_NAME, 8).join().properties.get("test"), 20.0);
    }

    public abstract AbstractUserService getUserService();

    public abstract Metastore getMetastore();
}
