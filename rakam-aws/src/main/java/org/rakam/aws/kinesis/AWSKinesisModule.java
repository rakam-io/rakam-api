package org.rakam.aws.kinesis;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.Config;
import org.rakam.aws.AWSConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "event.store", value = "kinesis")
public class AWSKinesisModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(AWSConfig.class);
        configBinder(binder).bindConfig(PrestoStreamConfig.class);
        binder.bind(EventStore.class).to(AWSKinesisEventStore.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "AWS Kinesis event store module";
    }

    @Override
    public String description() {
        return "Puts your events directly to AWS Kinesis streams.";
    }

    public static class PrestoStreamConfig {
        private int port;

        public int getPort() {
            return port;
        }

        @Config("presto.streaming.port")
        public void setPort(int port) {
            this.port = port;
        }
    }

}
