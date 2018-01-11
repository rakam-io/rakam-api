package org.rakam.kafka.collection;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class KafkaConfig {
    public static final String SERIALIZER = "kafka.serializer.DefaultEncoder";
    private static final int KAFKA_DEFAULT_PORT = 9092;
    private Set<HostAndPort> nodes = ImmutableSet.of();
    private Duration kafkaConnectTimeout = Duration.valueOf("10s");
    private DataSize kafkaBufferSize = new DataSize(64, DataSize.Unit.KILOBYTE);
    private Duration commitInterval = Duration.valueOf("5s");
    private HostAndPort zookeeperNode;

    public static ImmutableSet<HostAndPort> parseNodes(String nodes) {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), KafkaConfig::toHostAddress));
    }

    private static HostAndPort toHostAddress(String value) {
        return HostAndPort.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }

    //    @Size(min = 1)
    public Set<HostAndPort> getNodes() {
        return nodes;
    }

    @Config("nodes")
    public KafkaConfig setNodes(String nodes) {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    @MinDuration("1s")
    public Duration getKafkaConnectTimeout() {
        return kafkaConnectTimeout;
    }

    @Config("connect-timeout")
    public KafkaConfig setKafkaConnectTimeout(String kafkaConnectTimeout) {
        this.kafkaConnectTimeout = Duration.valueOf(kafkaConnectTimeout);
        return this;
    }

    public DataSize getKafkaBufferSize() {
        return kafkaBufferSize;
    }

    @Config("buffer-size")
    public KafkaConfig setKafkaBufferSize(String kafkaBufferSize) {
        this.kafkaBufferSize = DataSize.valueOf(kafkaBufferSize);
        return this;
    }

    @MinDuration("1s")
    public Duration getCommitInterval() {
        return commitInterval;
    }

    @Config("commit-interval")
    public KafkaConfig setCommitInterval(String interval) {
        if (interval != null)
            this.commitInterval = Duration.valueOf(interval);
        return this;
    }

    public HostAndPort getZookeeperNode() {
        return zookeeperNode;
    }

    @Config("zookeeper.connect")
    public KafkaConfig setZookeeperNode(String node) {
        this.zookeeperNode = (node == null) ? null : HostAndPort.fromString(node);
        return this;
    }
}

