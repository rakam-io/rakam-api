package org.rakam.clickhouse.collection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.avro.generic.GenericRecord;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.util.ProjectCollection;

import javax.ws.rs.core.UriBuilder;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rakam.clickhouse.analysis.ClickHouseQueryExecution.getSystemSocksProxy;
import static org.rakam.collection.FieldType.DATE;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.ValidationUtil.checkCollection;

public class ClickHouseEventStore
        implements EventStore {
    private final static Logger LOGGER = Logger.get(ClickHouseEventStore.class);

    private static final byte[] EMPTY_ARRAY = new byte[]{};
    private static final String EMPTY_STRING = "";
    final JettyHttpClient HTTP_CLIENT = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, SECONDS))
                    .setSocksProxy(getSystemSocksProxy()), new JettyIoPool("rakam-clickhouse", new JettyIoPoolConfig()),
            ImmutableSet.of());

    final Map<ProjectCollection, List<Event>> queuedEvents;
    private final ClickHouseConfig config;
    private final ProjectConfig projectConfig;
    Map<ProjectCollection, CompletableFuture<Void>> currentFutureSingle;

    @Inject
    public ClickHouseEventStore(ProjectConfig projectConfig, ClickHouseConfig config) {
        this.config = config;
        this.projectConfig = projectConfig;
        queuedEvents = new ConcurrentHashMap<>();
        currentFutureSingle = new ConcurrentHashMap<>();

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
            try {
                Iterator<Map.Entry<ProjectCollection, List<Event>>> iterator = queuedEvents.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<ProjectCollection, List<Event>> next = iterator.next();
                    List<Event> value = next.getValue();
                    iterator.remove();
                    CompletableFuture<Void> remove = currentFutureSingle.remove(next.getKey());

                    List<SchemaField> schema = value.get(0).schema();

                    executeRequest(next.getKey(), schema, next.getValue(), remove, false);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public static void writeValue(Object value, FieldType type, DataOutput out)
            throws IOException {
        switch (type) {
            case STRING:
                String str = value == null ? EMPTY_STRING : value.toString();
                writeVarInt(str.length(), out);
                out.writeBytes(str);
                break;
            case DATE:
                out.writeShort(value == null ? 0 : (Integer) value);
                break;
            case TIMESTAMP:
                out.writeInt(value == null ? 0 : ((int) ((Long) value / 1000)));
                break;
            case TIME:
            case INTEGER:
                out.writeInt(value == null ? 0 : (Integer) value);
                break;
            case DECIMAL:
            case DOUBLE:
                out.writeDouble(value == null ? .0 : ((Double) value));
                break;
            case LONG:
                out.writeLong(value == null ? 0L : ((Long) value));
                break;
            case BOOLEAN:
                out.write((value != null && value.equals(Boolean.TRUE)) ? 1 : 0);
                break;
            case BINARY:
                byte[] bytes = value == null ? EMPTY_ARRAY : (byte[]) value;
                writeVarInt(bytes.length, out);
                out.write(bytes);
                break;
            default:
                if (type.isArray()) {
                    List list = value == null ? ImmutableList.of() : (List) value;
                    writeVarInt(list.size(), out);
                    FieldType arrayElementType = type.getArrayElementType();

                    for (Object item : list) {
                        writeValue(item, arrayElementType, out);
                    }
                } else if (type.isMap()) {
                    Map<String, Object> map = value == null ? ImmutableMap.of() : (Map<String, Object>) value;
                    writeVarInt(map.size(), out);
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        writeValue(entry.getKey(), STRING, out);
                    }

                    FieldType mapValueType = type.getMapValueType();

                    writeVarInt(map.size(), out);
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        writeValue(entry.getValue(), mapValueType, out);
                    }
                } else {
                    throw new IllegalStateException();
                }
        }
    }

    public static void writeVarInt(int message, DataOutput output)
            throws IOException {
        // VarInts don't support negative values
        if (message < 0) {
            message = 0;
        }
        int value = message;
        while (value > 0x7f) {
            output.write((byte) ((value & 0x7f) | 0x80));
            value >>= 7;
        }
        output.write((byte) value);
    }

    private void executeRequest(ProjectCollection collection, List<SchemaField> schema, List<Event> events, CompletableFuture<Void> future, boolean tried) {
        HttpResponseFuture<StringResponse> f = HTTP_CLIENT.executeAsync(Request.builder()
                .setUri(buildInsertUri(collection, schema))
                .setMethod("POST")
                .setBodyGenerator(new BinaryRawGenerator(events, schema))
                .build(), createStringResponseHandler());

        f.addListener(() -> {
            try {
                StringResponse stringResponse = f.get(1L, MINUTES);
                if (stringResponse.getStatusCode() == 200) {
                    future.complete(null);
                } else {
                    RuntimeException ex = new RuntimeException(stringResponse.getStatusMessage() + " : "
                            + stringResponse.getBody().split("\n", 2)[0]);
                    future.completeExceptionally(ex);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                if (!tried) {
                    executeRequest(collection, schema, events, future, true);
                } else {
                    future.completeExceptionally(e);
                    LOGGER.error(e);
                }

            }
        }, Runnable::run);
    }

    private URI buildInsertUri(ProjectCollection collection, List<SchemaField> schema) {
        return UriBuilder
                .fromUri(config.getAddress())
                .queryParam("query", format("INSERT INTO %s.%s (`$date`, %s) FORMAT RowBinary",
                        collection.project, checkCollection(collection.collection, '`'),
                        schema.stream().flatMap(f -> f.getType().isMap() ? Stream.of(checkCollection(f.getName(), '`') + ".Key", checkCollection(f.getName(), '`') + ".Value") : Stream.of(checkCollection(f.getName(), '`')))
                                .collect(Collectors.joining(", ")))).build();
    }

    @Override
    public CompletableFuture<int[]> storeBatchAsync(List<Event> events) {
        CompletableFuture[] futures = new CompletableFuture[events.size()];

        for (int i = 0; i < events.size(); i++) {
            futures[i] = storeAsync(events.get(i));
        }

        return CompletableFuture.allOf(futures).thenApply(v -> {
            List<Integer> ints = null;
            for (int i = 0; i < futures.length; i++) {
                if (futures[i].isCompletedExceptionally()) {
                    if (ints == null) {
                        ints = new ArrayList();
                        ints.add(i);
                    }
                }
            }

            if (ints == null) {
                return SUCCESSFUL_BATCH;
            } else {
                return Ints.toArray(ints);
            }
        });
    }

    @Override
    public CompletableFuture<Void> storeAsync(Event event) {
        ProjectCollection tuple = new ProjectCollection(event.project(), event.collection());
        CompletableFuture<Void> future = currentFutureSingle.computeIfAbsent(tuple, (k) -> new CompletableFuture<>());

        queuedEvents.computeIfAbsent(tuple, (k) -> Collections.synchronizedList(new ArrayList<>())).add(event);
        return future;
    }

    private class BinaryRawGenerator
            implements BodyGenerator {
        private final List<Event> value;
        private final List<SchemaField> schema;

        public BinaryRawGenerator(List<Event> value, List<SchemaField> schema) {
            this.value = value;
            this.schema = schema;
        }

        @Override
        public void write(OutputStream outputStream)
                throws Exception {
            LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(outputStream);

            for (Event event : value) {
                GenericRecord record = event.properties();
                Object time = record.get(projectConfig.getTimeColumn());
                writeValue(time == null ? 0 : ((int) (((long) time) / 86400)), DATE, out);

                for (int i = 0; i < schema.size(); i++) {
                    writeValue(record.get(i), schema.get(i).getType(), out);
                }
            }
        }
    }
}
