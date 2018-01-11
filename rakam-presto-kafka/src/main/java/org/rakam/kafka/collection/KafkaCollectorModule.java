package org.rakam.kafka.collection;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.stream.EventStream;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

@AutoService(RakamModule.class)
@ConditionalModule(config = "event.store", value = "kafka")
public class KafkaCollectorModule extends RakamModule {

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
        return "Kafka Event Collector";
    }

    @Override
    public String description() {
        return "Sends events to Apache Kafka";
    }
}
