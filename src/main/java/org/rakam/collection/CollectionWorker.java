package org.rakam.collection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.inject.Binding;
import com.google.inject.TypeLiteral;
import org.apache.log4j.Logger;
import org.rakam.ServiceStarter;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.local.LocalCacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.rakam.plugin.CollectionMapperPlugin;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by buremba on 21/05/14.
 */
public class CollectionWorker extends Verticle implements Handler<Message<byte[]>> {
    final ExecutorService executor = new ThreadPoolExecutor(5, 50, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());

    public static CacheAdapter localStorageAdapter = new LocalCacheAdapter();
    private CacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);
    private DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);

    final private EventAggregator eventAggregator = new EventAggregator(localStorageAdapter, cacheAdapter, databaseAdapter);

    Kryo kryo = new Kryo();
    static Logger logger = Logger.getLogger(CollectionWorker.class.getName());
    static List<CollectionMapperPlugin> mappers = new LinkedList();
    static {
        List<Binding<CollectionMapperPlugin>> bindingsByType = ServiceStarter.injector.findBindingsByType(new TypeLiteral<CollectionMapperPlugin>() {});
        mappers.addAll(bindingsByType.stream().map(mapper -> mapper.getProvider().get()).collect(Collectors.toList()));
    }

    public void start() {
        vertx.eventBus().registerLocalHandler("collectEvent", this);
        vertx.eventBus().registerLocalHandler("collectActor", this);
        kryo.register(JsonObject.class);
    }

    @Override
    public void handle(final Message<byte[]> data) {
        if(data.address().equals("collectEvent")) {
            executor.submit(createEventCollectionTask(data));
        }else
        if(data.address().equals("collectActor")) {
            executor.submit(createActorCollectionTask(data));
        }else {
            logger.error("unknown collection type");
            data.reply("0".getBytes());
        }
    }

    private Runnable createEventCollectionTask(final Message<byte[]> data) {
        return () -> {
            try {
                final JsonObject message = kryo.readObject(new Input(data.body()), JsonObject.class);

                mappers.stream().filter(mapper -> !mapper.map(message)).forEach(mapper -> data.reply("0".getBytes()));

                String project = message.getString("_tracker");
                String actor_id = message.getString("_user");

                Iterator<String> it = message.getFieldNames().iterator();
                while(it.hasNext()) {
                    String key = it.next();
                    if (key.startsWith("_"))
                        it.remove();
                }

                databaseAdapter.addEvent(project, actor_id, message);

                eventAggregator.aggregate(project, message, actor_id, (int) (System.currentTimeMillis() / 1000));

                data.reply("1".getBytes());
            } catch (Exception e) {
                data.reply("0".getBytes());
                logger.error("error while processing event collection request", e);
            }
        };
    }

    public Runnable createActorCollectionTask(final Message<byte[]> data) {
        return () -> {
            try {
                final JsonObject message = kryo.readObject(new Input(data.body()), JsonObject.class);

                String tracker = message.getString("tracker");
                String actor_id = message.getString("user");

                JsonObject attributes = message.getObject("attrs");
                databaseAdapter.addPropertyToActor(tracker, actor_id, attributes.toMap());
                data.reply("1".getBytes());
            } catch (Exception e) {
                data.reply("0".getBytes());
                logger.error("error while processing event collection request", e);
            }
        };
    }


}

