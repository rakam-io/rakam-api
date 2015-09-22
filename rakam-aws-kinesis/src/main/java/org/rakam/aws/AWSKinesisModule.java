package org.rakam.aws;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.rakam.aws.KinesisEventStream.EventStreamService;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.kume.ClusterMembership;
import org.rakam.kume.JoinerService;
import org.rakam.kume.Member;
import org.rakam.kume.service.ServiceInitializer;
import org.rakam.kume.util.NetworkUtil;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.EventStreamConfig;
import org.rakam.plugin.RakamModule;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

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
        binder.bind(EventStore.class).to(AWSKinesisEventStore.class).in(Scopes.SINGLETON);
        if (buildConfigObject(EventStreamConfig.class).isEventStreamEnabled()) {
            binder.bind(HazelcastInstance.class).toProvider(HazelcastInstanceProvider.class)
                    .in(Scopes.SINGLETON);
            binder.bind(EventStream.class).to(KinesisEventStream.class).in(Scopes.SINGLETON);
            binder.bind(Cluster.class).toProvider(KumeClusterProvider.class)
                    .in(Scopes.SINGLETON);
        }
    }

    @Override
    public String name() {
        return "AWS Kinesis event store module";
    }

    @Override
    public String description() {
        return "Puts your events directly to AWS Kinesis streams";
    }


    protected static class KumeClusterProvider implements Provider<Cluster> {

        private final HazelcastInstance hazelcast;
        private final Metastore metastore;

        @Inject
        public KumeClusterProvider(HazelcastInstance hazelcast, Metastore metastore) {
            this.hazelcast = hazelcast;
            this.metastore = metastore;
        }

        @Override
        public Cluster get() {

            List<Member> collect = hazelcast.getCluster().getMembers().stream()
                    .map(mem -> {
                        String addr = mem.getStringAttribute("kumeSocketAddress");
                        int endIndex = addr.indexOf(':');
                        return new Member(addr.substring(0, endIndex), Integer.parseInt(addr.substring(endIndex+1)));
                    })
                    .collect(Collectors.toList());

            Cluster cluster = new ClusterBuilder()
                    .members(collect)
                    .services(new ServiceInitializer()
                            .add("eventListener", bus -> new EventStreamService(bus, metastore)))
                    .serverAddress(NetworkUtil.getDefaultAddress(), 5656)
                    .joinStrategy(new HazelcastJoinerStrategy(hazelcast))
                    .mustJoinCluster(true)
                    .client(true)
                    .start();
            hazelcast.getSet("clients").add(cluster.getLocalMember());

            return cluster;
        }
    }

    protected static class HazelcastInstanceProvider implements Provider<HazelcastInstance> {
        private final AWSConfig awsConfig;

        @Inject
        HazelcastInstanceProvider(AWSConfig config) {
            this.awsConfig = config;
        }

        @Override
        public HazelcastInstance get() {
            ClientConfig config = new ClientConfig();
            config.getNetworkConfig().addAddress("127.0.0.1");
//            AwsConfig hazelcastAwsConfig = join.getAwsConfig();
//            hazelcastAwsConfig.setAccessKey(awsConfig.getAccessKey());
//            hazelcastAwsConfig.setSecretKey(awsConfig.getSecretAccessKey());
//            hazelcastAwsConfig.setSecurityGroupName("test");
//            hazelcastAwsConfig.setEnabled(true);

            config.getSerializationConfig()
                    .setGlobalSerializerConfig(
                            new GlobalSerializerConfig().setImplementation(new SchemaFieldStreamSerializer()));

            return HazelcastClient.newHazelcastClient(config);
        }

        private static class SchemaFieldStreamSerializer implements StreamSerializer<Object> {

            private final KryoFactory factory;
            private final KryoPool pool;

            public SchemaFieldStreamSerializer() {
                factory = () -> {
                    Kryo kryo = new Kryo();
                    UnmodifiableCollectionsSerializer.registerSerializers(kryo);
                    ImmutableListSerializer.registerSerializers(kryo);
                    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
                    return kryo;
                };
                pool = new KryoPool.Builder(factory).softReferences().build();
            }

            @Override
            public int getTypeId() {
                return 21;
            }

            @Override
            public void destroy() {

            }

            @Override
            public void write(ObjectDataOutput out, Object object) throws IOException {
                Kryo borrow = pool.borrow();
                Output output = new Output(new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        out.write(b);
                    }
                });
                borrow.writeClassAndObject(output, object);
                output.flush();
                pool.release(borrow);
            }

            @Override
            public Object read(ObjectDataInput in) throws IOException {
                Kryo borrow = pool.borrow();
                Object obj = borrow.readClassAndObject(new Input(new InputStream() {
                    @Override
                    public int read() throws IOException {
                        return in.readByte();
                    }
                }));
                pool.release(borrow);
                return obj;
            }

        }
    }

    private static class HazelcastJoinerStrategy implements JoinerService {
        HazelcastInstance hazelcastInstance;

        public HazelcastJoinerStrategy(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void onStart(ClusterMembership clusterMembership) {
            hazelcastInstance.getCluster().addMembershipListener(new MembershipListener() {
                @Override
                public void memberAdded(MembershipEvent membershipEvent) {
                    String addr = membershipEvent.getMember().getStringAttribute("kumeSocketAddress");
                    int endIndex = addr.indexOf(':');
                    Member member = new Member(addr.substring(0, endIndex), Integer.parseInt(addr.substring(endIndex + 1)));
                    clusterMembership.addMember(member);
                }

                @Override
                public void memberRemoved(MembershipEvent membershipEvent) {
//                    clusterMembership.removeMember(new Member(membershipEvent.getMember().getSocketAddress()));
                }

                @Override
                public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

                }
            });
        }

        @Override
        public void onClose() {

        }
    }
}
