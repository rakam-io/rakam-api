package org.rakam.plugin;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import org.rakam.collection.Event;
import org.rakam.util.ConditionalModule;
import org.rakam.util.RakamException;

import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;

public class DummyEventStore
        implements SyncEventStore {
    @Override
    public void store(Event event) {
        throw new RakamException(NOT_IMPLEMENTED);
    }

    @Override
    public int[] storeBatch(List<Event> events) {
        throw new RakamException(NOT_IMPLEMENTED);
    }

    @AutoService(RakamModule.class)
    @ConditionalModule(config = "event.store", value = "dummy")
    public static class DummyEventStoreModule
            extends RakamModule {

        @Override
        protected void setup(Binder binder) {
            binder.bind(EventStore.class).to(DummyEventStore.class);
        }

        @Override
        public String name() {
            return "Dummy Event Store";
        }

        @Override
        public String description() {
            return "Used if no event store implementation exists";
        }
    }
}
