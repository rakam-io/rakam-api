package org.rakam.aws;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.hazelcast.config.Config;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.RakamModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
        binder.bind(EventStream.class).to(KinesisEventStream.class).in(Scopes.SINGLETON);
        binder.bind(HazelcastInstance.class).toProvider(HazelcastInstanceProvider.class)
                .in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "AWS Kinesis event store module";
    }

    @Override
    public String description() {
        return "Puts your events directly to AWS Kinesis streams";
    }


    protected static class HazelcastInstanceProvider implements Provider<HazelcastInstance> {
        private final AWSConfig awsConfig;

        @Inject
        HazelcastInstanceProvider(AWSConfig config) {
            this.awsConfig = config;
        }

        @Override
        public HazelcastInstance get() {
            Config config = new Config();
            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true);
            join.getTcpIpConfig().addMember("127.0.0.1");
            config.getNetworkConfig().getInterfaces().clear();
            config.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
            config.getNetworkConfig().getInterfaces().setEnabled(true);
//            AwsConfig hazelcastAwsConfig = join.getAwsConfig();
//            hazelcastAwsConfig.setAccessKey(awsConfig.getAccessKey());
//            hazelcastAwsConfig.setSecretKey(awsConfig.getSecretAccessKey());
//            hazelcastAwsConfig.setSecurityGroupName("test");
//            hazelcastAwsConfig.setEnabled(true);

            config.getSerializationConfig()
                    .setGlobalSerializerConfig(
                            new GlobalSerializerConfig().setImplementation(new SchemaFieldStreamSerializer()));

            return Hazelcast.newHazelcastInstance(config);
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
}
