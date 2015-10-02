package org.rakam.aws;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.airlift.configuration.Config;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterMembership;
import org.rakam.kume.JoinerService;
import org.rakam.kume.Member;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.EventStreamConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.report.PrestoConfig;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 06:40.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config="event.store", value="kinesis")
public class AWSKinesisModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(AWSConfig.class);
        bindConfig(binder).to(KumeConfig.class);
        binder.bind(EventStore.class).to(AWSKinesisEventStore.class).in(Scopes.SINGLETON);
        if (buildConfigObject(EventStreamConfig.class).isEventStreamEnabled()) {
            binder.bind(Cluster.class).toProvider(KumeClusterInstanceProvider.class).in(Scopes.SINGLETON);
            binder.bind(EventStream.class).toProvider(KinesisEventStreamProvider.class).in(Scopes.SINGLETON);
        }
    }

    @Override
    public String name() {
        return "AWS Kinesis event store module";
    }

    @Override
    public String description() {
        return "Puts your events directly to AWS Kinesis streams.";
    }

    public static class KumeConfig {
        private int port;

        @Config("kinesis.streamer.kume.port")
        public void setPort(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }
    }

    protected  static class KinesisEventStreamProvider implements Provider<EventStream> {

        private final Cluster cluster;

        @Inject
        public KinesisEventStreamProvider(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public EventStream get() {
            return cluster.getService("eventStreamer");
        }
    }


    public static class KumeJoinerService implements JoinerService {
        private final URL coordinatorAddress;
        private final KumeConfig config;
        private ClusterMembership membership;
        private Set<String> nodes;

        public KumeJoinerService(PrestoConfig prestoConfig, KumeConfig config) {
            this.config = config;
            try {
                URI address = prestoConfig.getAddress();
                this.coordinatorAddress = new URL(address.toURL().toExternalForm()+
                        "/v1/node");
            } catch (MalformedURLException e) {
                throw Throwables.propagate(e);
            }
            nodes = ImmutableSet.of();
        }

        @Override
        public void onStart(ClusterMembership membership) {
            this.membership = membership;
            Executors.newSingleThreadScheduledExecutor()
                    .scheduleAtFixedRate(this::updateNode, 1, 1, TimeUnit.SECONDS);
        }

        private void updateNode() {

            try {
                ArrayNode result;
                try {
                    URLConnection yc = coordinatorAddress.openConnection();
                    byte[] json = ByteStreams.toByteArray(yc.getInputStream());
                    result = JsonHelper.read(json, ArrayNode.class);
                } catch (IOException e) {
                    return;
                }

                Set<String> activeNodes = StreamSupport.stream(result.spliterator(), false)
                            .map(node -> URI.create(node.get("uri").asText()).getHost()).collect(Collectors.toSet());
                activeNodes.add(coordinatorAddress.getHost());

                Set<String> removedNodes = nodes.stream().filter(node -> !activeNodes.contains(node)).collect(Collectors.toSet());
                Set<String> newNodes = activeNodes.stream().filter(node -> !nodes.contains(node)).collect(Collectors.toSet());

                newNodes.forEach(node -> membership.addMember(new Member(node, config.getPort())));
                removedNodes.forEach(node -> membership.removeMember(new Member(node, config.getPort())));
                nodes = newNodes;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
