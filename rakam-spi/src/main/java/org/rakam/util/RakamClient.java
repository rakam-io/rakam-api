package org.rakam.util;

import autovalue.shaded.com.google.common.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import okhttp3.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RakamClient {
    public static final String RELEASE;
    public static final String HOST_NAME;
    private static final Logger logger = Logger.get(RakamClient.class);
    private static final String JAVA_VERSION;
    private static final String OS_NAME;
    private static final String OS_ARCHITECTURE;
    private static final String OS_VERSION;
    private static final int AVAILABLE_PROCESSOR;
    private static final long TOTAL_JVM_MEMORY;
    private static final String url = "https://pool1.rakam.io/event/collect";
    private static final OkHttpClient client = new OkHttpClient();
    private static final MediaType mediaType = MediaType.parse("application/json");

    static {
        URL gitProps = LogUtil.class.getResource("/git.properties");

        if (gitProps != null) {
            Properties properties = new Properties();
            try {
                properties.load(gitProps.openStream());
            } catch (IOException e) {
            }

            RELEASE = properties.get("git.commit.id.describe").toString();
        } else {
            RELEASE = null;
        }

        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            logger.warn(e, "Not found host address");
            hostName = null;
        }

        HOST_NAME = hostName;

        JAVA_VERSION = System.getProperty("java.version");
        OS_NAME = System.getProperty("os.name ");
        OS_ARCHITECTURE = System.getProperty("os.arch");
        OS_VERSION = System.getProperty("os.version");
        AVAILABLE_PROCESSOR = Runtime.getRuntime().availableProcessors();
        TOTAL_JVM_MEMORY = Runtime.getRuntime().totalMemory();
    }

    public static void logEvent(String collection, Map<String, Object> map) {
        try {
            map = new HashMap<>(map);

            map.put("system.java_version", JAVA_VERSION);
            map.put("system.os_name", OS_NAME);
            map.put("system.os_architecture", OS_ARCHITECTURE);
            map.put("system.os_version", OS_VERSION);
            map.put("system.available_processor", AVAILABLE_PROCESSOR);
            map.put("system.total_jvm_memory", TOTAL_JVM_MEMORY);
            map.put("system.release", HOST_NAME);

            map.put("app.release", RELEASE);

            RequestBody body = RequestBody.create(mediaType,
                    String.format("{\"api\":{\"api_key\": \"%s\", \"upload_time\":%d},\"collection\":%s, \"properties\": %s}",
                            "n7nniqm417rk87g6o622dbtddtaa2i3663i2msl2q7ildlet2gm6gg9rg9abu45a", Instant.now().toEpochMilli(),
                            JsonHelper.encode(collection), JsonHelper.encode(map)));

            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    logger.warn(e, "Error while logging event %s", collection);
                }

                @Override
                public void onResponse(Call call, Response response)
                        throws IOException {
                    logger.debug("Successfully logged %s event", collection);
                    response.close();
                }
            });
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public static void logEvent(String collection) {
        logEvent(collection, ImmutableMap.of());
    }
}
