package org.rakam.analysis.webhook;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import okhttp3.*;
import org.rakam.aws.AWSConfig;
import org.rakam.collection.Event;
import org.rakam.collection.EventList;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventMapper;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class WebhookEventMapper implements EventMapper {
    private final static Logger LOGGER = Logger.get(WebhookEventMapper.class);

    private final OkHttpClient asyncHttpClient;
    private final static int TIMEOUT_IN_MILLIS = 10000;
    private final Queue<Event> queue = new ConcurrentLinkedQueue<>();
    private final DynamicSliceOutput slice;
    private final AtomicInteger counter;
    private final AmazonCloudWatchAsyncClient cloudWatchClient;

    @Inject
    public WebhookEventMapper(WebhookConfig config, AWSConfig awsConfig) {
        this.asyncHttpClient = new OkHttpClient.Builder()
                .connectTimeout(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS)
                .readTimeout(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS)
                .writeTimeout(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS)
                .build();
        slice = new DynamicSliceOutput(100);
        counter = new AtomicInteger();
        cloudWatchClient = new AmazonCloudWatchAsyncClient(awsConfig.getCredentials());
        cloudWatchClient.setRegion(awsConfig.getAWSRegion());

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("collection-webhook").build());
        service.scheduleAtFixedRate(() -> {
            try {
                int size = counter.get();
                if (size == 0) {
                    return;
                }

                JsonGenerator generator = JsonHelper.getMapper().getFactory().createGenerator((DataOutput) slice);

                generator.writeStartObject();
                generator.writeFieldName("activities");

                generator.writeStartArray();

                int i = 0;
                for (; i < size; i++) {
                    Event event = queue.poll();
                    if (event == null) {
                        break;
                    }
                    generator.writeStartObject();
                    generator.writeFieldName("collection");
                    generator.writeString(event.collection());

                    List<SchemaField> fields = event.schema();
                    for (SchemaField field : fields) {
                        generator.writeFieldName("properties." + field.getName());
                        Object value = event.getAttribute(field.getName());
                        if (value == null) {
                            generator.writeNull();
                        } else {
                            write(field.getType(), generator, value);
                        }
                    }

                    generator.writeEndObject();
                }

                generator.writeEndArray();
                generator.writeEndObject();

                generator.flush();
                byte[] base = (byte[]) slice.getUnderlyingSlice().getBase();
                MediaType mediaType = MediaType.parse("application/json");
                Request.Builder builder = new Request.Builder().url(config.getUrl());
                if (config.getHeaders() != null) {
                    for (Map.Entry<String, String> entry : config.getHeaders().entrySet()) {
                        builder.addHeader(entry.getKey(), entry.getValue());
                    }
                }

                Request build = builder.post(RequestBody.create(mediaType, base, 0, slice.size())).build();
                tryOperation(build, 2, i);
                counter.addAndGet(-i);
            } catch (Throwable e) {
                LOGGER.error(e, "Error while sending request to webhook");
                slice.reset();
            }
        }, 5, 5, SECONDS);
    }

    private void write(FieldType type, JsonGenerator generator, Object value) throws IOException {
        switch (type) {
            case STRING:
            case BOOLEAN:
            case LONG:
            case INTEGER:
            case DECIMAL:
            case DOUBLE:
            case TIMESTAMP:
            case TIME:
            case DATE:
                generator.writeString(value.toString());
                break;
            default:
                if (type.isMap()) {
                    generator.writeNull();
                } else
                if (type.isArray()) {
                    generator.writeStartArray();

                    for (Object item : ((List) value)) {
                        generator.writeString(item.toString());
                    }

                    generator.writeEndArray();
                } else {
                    throw new IllegalStateException(format("type %s is not supported.", type));
                }
        }
    }

    private void tryOperation(Request build, int retryCount, int numberOfRecords) throws IOException {
        Response execute = null;

        try {
            execute = asyncHttpClient.newCall(build).execute();

            if (execute.code() != 200) {
                if (retryCount > 0) {
                    tryOperation(build, retryCount - 1, numberOfRecords);
                    return;
                }

                cloudWatchClient.putMetricDataAsync(new PutMetricDataRequest()
                        .withNamespace("rakam-webhook")
                        .withMetricData(new MetricDatum()
                                .withMetricName("request-error")
                                .withValue(Double.valueOf(numberOfRecords))));

                LOGGER.warn(new RuntimeException(execute.body().string()), "Unable to execute Webhook request");
            } else {
                cloudWatchClient.putMetricDataAsync(new PutMetricDataRequest()
                        .withNamespace("rakam-webhook")
                        .withMetricData(new MetricDatum()
                                .withMetricName("request-success")
                                .withValue(Double.valueOf(numberOfRecords))));

                cloudWatchClient.putMetricDataAsync(new PutMetricDataRequest()
                        .withNamespace("rakam-webhook")
                        .withMetricData(new MetricDatum()
                                .withMetricName("request-latency")
                                .withValue(Double.valueOf(execute.receivedResponseAtMillis() - execute.sentRequestAtMillis()))));
            }
        } finally {
            if (execute != null) {
                execute.close();
            }
            slice.reset();
        }
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        queue.add(event);
        counter.incrementAndGet();
        return COMPLETED_EMPTY_FUTURE;
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(EventList events, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        queue.addAll(events.events);
        counter.addAndGet(events.events.size());
        return COMPLETED_EMPTY_FUTURE;
    }
}
