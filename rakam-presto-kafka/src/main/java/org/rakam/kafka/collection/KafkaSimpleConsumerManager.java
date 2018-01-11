package org.rakam.kafka.collection;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HostAndPort;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import kafka.javaapi.consumer.SimpleConsumer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
public class KafkaSimpleConsumerManager {

    private final static Logger LOGGER = Logger.get(KafkaSimpleConsumerManager.class);

    private final LoadingCache<HostAndPort, SimpleConsumer> consumerCache;

    @Inject
    KafkaSimpleConsumerManager() {
        this.consumerCache = CacheBuilder.newBuilder().build(new SimpleConsumerCacheLoader());
    }

    @PreDestroy
    public void tearDown() {
        for (Map.Entry<HostAndPort, SimpleConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.warn("While closing consumer %s:", entry.getKey(), e);
            }
        }
    }

    public SimpleConsumer getConsumer(HostAndPort host) {
        checkNotNull(host, "host is null");
        try {
            return consumerCache.get(host);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class SimpleConsumerCacheLoader
            extends CacheLoader<HostAndPort, SimpleConsumer> {
        @Override
        public SimpleConsumer load(HostAndPort host)
                throws Exception {
            LOGGER.info("Creating new Consumer for {}", host);
            return new SimpleConsumer(host.getHostText(),
                    host.getPort(),
                    10000,
                    1024,
                    "consumer");
        }
    }
}
