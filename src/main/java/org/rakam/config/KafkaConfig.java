package org.rakam.config;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import org.rakam.util.HostAddress;

import javax.validation.constraints.Size;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 01:42.
 */
public class KafkaConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;
    public static final String SERIALIZER = "kafka.serializer.DefaultEncoder";

    private Set<HostAddress> nodes = ImmutableSet.of();
    private Duration kafkaConnectTimeout = Duration.valueOf("10s");
    private DataSize kafkaBufferSize = new DataSize(64, DataSize.Unit.KILOBYTE);

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("nodes")
    public KafkaConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    @MinDuration("1s")
    public Duration getKafkaConnectTimeout()
    {
        return kafkaConnectTimeout;
    }

    @Config("connect-timeout")
    public KafkaConfig setKafkaConnectTimeout(String kafkaConnectTimeout)
    {
        this.kafkaConnectTimeout = Duration.valueOf(kafkaConnectTimeout);
        return this;
    }

    public DataSize getKafkaBufferSize()
    {
        return kafkaBufferSize;
    }

    @Config("buffer-size")
    public KafkaConfig setKafkaBufferSize(String kafkaBufferSize)
    {
        this.kafkaBufferSize = DataSize.valueOf(kafkaBufferSize);
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), KafkaConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }
}

