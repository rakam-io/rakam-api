package org.rakam.aws;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.EventStreamConfig;
import org.rakam.plugin.RakamModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config="event.store", value="kinesis")
public class AWSKinesisModule extends RakamModule {
    private final static Logger LOGGER = Logger.get(AWSKinesisModule.class);

    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(AWSConfig.class);
        configBinder(binder).bindConfig(PrestoStreamConfig.class);
        binder.bind(EventStore.class).to(AWSKinesisEventStore.class).in(Scopes.SINGLETON);
        if (buildConfigObject(EventStreamConfig.class).isEventStreamEnabled()) {
            httpClientBinder(binder).bindHttpClient("streamer", ForStreamer.class);
            binder.bind(EventStream.class).to(KinesisEventStream.class).in(Scopes.SINGLETON);
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

    public static class PrestoStreamConfig {
        private int port;

        @Config("presto.streaming.port")
        public void setPort(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }
    }

}
