package org.rakam.plugin.realtime;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import kafka.javaapi.consumer.SimpleConsumer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/02/15 02:46.
 */
public class KafkaSimpleConsumerManager
{

    private final LoadingCache<InetSocketAddress, SimpleConsumer> consumerCache;


    @Inject
    KafkaSimpleConsumerManager()
    {
        this.consumerCache = CacheBuilder.newBuilder().build(new SimpleConsumerCacheLoader());
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<InetSocketAddress, SimpleConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
//                log.warn(e, "While closing consumer %s:", entry.getKey());
            }
        }
    }

    public SimpleConsumer getConsumer(InetSocketAddress host)
    {
        checkNotNull(host, "host is null");
        try {
            return consumerCache.get(host);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class SimpleConsumerCacheLoader
            extends CacheLoader<InetSocketAddress, SimpleConsumer>
    {
        @Override
        public SimpleConsumer load(InetSocketAddress host)
                throws Exception
        {
//            log.info("Creating new Consumer for %s", host);
            return new SimpleConsumer(host.getHostString(),
                    host.getPort(),
                    10000,
                    1024,
                    "consumer");
        }
    }
}
