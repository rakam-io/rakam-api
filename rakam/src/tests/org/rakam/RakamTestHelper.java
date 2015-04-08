package org.rakam;

import org.rakam.util.json.JsonObject;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 16:52.
 */
public class RakamTestHelper {
    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT = 120;

    private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static Random rnd = new Random();

    public static String randomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }

    public JsonObject randomJson(int entryCount, int keyLength, int valueLength) {
        JsonObject json = new JsonObject();
        for (int i = 0; i < entryCount; i++) {
            json.put(randomString(keyLength), randomString(valueLength));
        }
        return json;
    }

    public JsonObject iterativeJson(int entryCount, String keyPrefix, String valuePrefix) {
        JsonObject json = new JsonObject();
        for (int i = 0; i < entryCount; i++) {
            json.put(keyPrefix+i, valuePrefix+i);
        }
        return json;
    }

    public static void sleepSeconds(int seconds) {
        try {
            Thread.sleep(seconds*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void assertOpenEventually(CountDownLatch latch, String message, int timeoutSeconds) {
        try {
            boolean completed = latch.await(timeoutSeconds, TimeUnit.SECONDS);
            assertTrue(format("%s, CountDownLatch failed to complete within %d seconds , position left: %d", message, timeoutSeconds,
                    latch.getCount()), completed);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void assertOpenEventually(CountDownLatch latch) {
        assertOpenEventually(latch, "Timeout error", ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public void assertEqualsFunction(Object obj, Callable func, JsonObject json) {
        Object call = null;
        try {
            call = func.call();
        } catch (Exception e) {
            fail("json object doesn't have the you provided: "+json.encode());
        }
        String error = json.getString("error");
        if(error!=null)
            fail("json field has an error field: "+json.encode());
        if(obj instanceof Number) {
            obj = ((Number) obj).longValue();
        }
        if(call instanceof Number) {
            call = ((Number) call).longValue();
        }
        assertEquals(obj, call);
    }
}
