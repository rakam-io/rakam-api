package org.rakam.aws;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.RakamModule;

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
    }

    @Override
    public String name() {
        return "AWS Kinesis event store module";
    }

    @Override
    public String description() {
        return "Puts your events directly to AWS Kinesis streams";
    }
}
