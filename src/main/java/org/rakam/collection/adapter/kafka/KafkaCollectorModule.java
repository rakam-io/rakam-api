package org.rakam.collection.adapter.kafka;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.analysis.stream.EventStream;
import org.rakam.collection.event.EventStore;
import org.rakam.config.ConditionalModule;
import org.rakam.config.KafkaConfig;
import org.rakam.plugin.RakamModule;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:21.
 */
@AutoService(RakamModule.class)
public class KafkaCollectorModule extends RakamModule implements ConditionalModule {

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder)
                .annotatedWith(Names.named("event.store.kafka"))
                .prefixedWith("event.store.kafka")
                .to(KafkaConfig.class);
        binder.bind(EventStore.class).to(KafkaEventStore.class);
        binder.bind(EventStream.class).to(KafkaStream.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public boolean shouldInstall(ConfigurationFactory config) {
        return config.getProperties().get("event.store").toLowerCase().trim().equals("kafka");
    }
}
