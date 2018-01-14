package org.rakam.analysis.stream;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.stream.EventStream;
import org.rakam.util.ConditionalModule;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

@AutoService(RakamModule.class)
@ConditionalModule(config = "event-stream", value = "server")
public class APIEventStreamModule
        extends RakamModule {
    private static final int MAXIMUM_QUEUE_CAPACITY = 1000;

    @Override
    protected void setup(Binder binder) {
        binder.bind(new TypeLiteral<Map<String, List<CollectionStreamHolder>>>() {
        })
                .toInstance(new ConcurrentHashMap<>());
        binder.bind(EventStream.class).to(APIEventStream.class);
        binder.bind(ExpressionCompiler.class).in(Scopes.SINGLETON);
        Multibinder<EventMapper> mapperMultibinder = Multibinder.newSetBinder(binder, EventMapper.class);
        mapperMultibinder.addBinding().to(EventListenerMapper.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    public static class CollectionStreamHolder {
        public final List<CollectionFilter> collections;
        public final Queue<Event> messageQueue;

        public CollectionStreamHolder(List<CollectionFilter> collections) {
            this.collections = collections;
            this.messageQueue = new ArrayBlockingQueue<>(MAXIMUM_QUEUE_CAPACITY);
        }

        public static class CollectionFilter {
            public final String collection;
            public final Predicate<GenericRecord> filter;

            public CollectionFilter(String collection, Predicate<GenericRecord> filter) {
                this.collection = collection;
                this.filter = filter;
            }
        }
    }
}
