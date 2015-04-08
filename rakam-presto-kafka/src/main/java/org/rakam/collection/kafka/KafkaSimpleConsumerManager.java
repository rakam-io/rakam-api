package org.rakam.collection.kafka;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Singleton;
import kafka.javaapi.consumer.SimpleConsumer;
import org.rakam.util.HostAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/02/15 02:46.
 */
@Singleton
public class KafkaSimpleConsumerManager {

    final static Logger LOGGER = LoggerFactory.getLogger(KafkaSimpleConsumerManager.class);

    private final LoadingCache<HostAddress, SimpleConsumer> consumerCache;

    @Inject
    KafkaSimpleConsumerManager() {
        this.consumerCache = CacheBuilder.newBuilder().build(new SimpleConsumerCacheLoader());
    }

    @PreDestroy
    public void tearDown() {
        for (Map.Entry<HostAddress, SimpleConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.warn("While closing consumer %s:", entry.getKey(), e);
            }
        }
    }

    public SimpleConsumer getConsumer(HostAddress host) {
        checkNotNull(host, "host is null");
        try {
            return consumerCache.get(host);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class SimpleConsumerCacheLoader
            extends CacheLoader<HostAddress, SimpleConsumer> {
        @Override
        public SimpleConsumer load(HostAddress host)
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
