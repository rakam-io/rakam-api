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
import org.rakam.database.KeyValueStorage;
import org.rakam.plugin.CollectionMapperPlugin;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by buremba on 21/05/14.
 */
public class CollectionWorker extends Verticle implements Handler<Message<byte[]>> {
    final ExecutorService executor = new ThreadPoolExecutor(5, 50, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    public static KeyValueStorage activeStorageAdapter = new LocalCacheAdapter();
    private CacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);
    private DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);
    final private Aggregator aggregator = new Aggregator(activeStorageAdapter, cacheAdapter, databaseAdapter);
    Kryo kryo = new Kryo();
    static Logger logger = Logger.getLogger(CollectionWorker.class.getName());
    List<CollectionMapperPlugin> mappers;

    public void start() {
        vertx.eventBus().registerLocalHandler("collectionRequest", this);
        kryo.register(JsonObject.class);
        mappers = new ArrayList();
        for(Binding<CollectionMapperPlugin> mapper : ServiceStarter.injector.findBindingsByType(new TypeLiteral<CollectionMapperPlugin>() {})) {
            mappers.add(mapper.getProvider().get());
        }

    }

    @Override
    public void handle(final Message<byte[]> data) {
        Input in = new Input(data.body());
        final JsonObject message = kryo.readObject(in, JsonObject.class);

        for(CollectionMapperPlugin mapper : mappers) {
            if(!mapper.map(message))
                data.reply("0".getBytes());
        }

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try{
                    data.reply(process(message));
                } catch (Exception e) {
                    logger.error("error while processing collection request", e);
                }
            }
        });
    }

    public byte[] process(final JsonObject message) {
        String project = message.getString("_tracker");
        String actor_id = message.getString("_user");

        Iterator<String> it = message.getFieldNames().iterator();
        while(it.hasNext()) {
            String key = it.next();
            if (key.startsWith("_"))
                it.remove();
        }

         /*
            The current implementation change cabins monthly.
            However the interval must be determined by the system by taking account of data frequency
         */

        /*ByteArrayOutputStream by = new ByteArrayOutputStream();
        Output out = new Output(by, 150);
        kryo.writeObject(out, message);*/
        databaseAdapter.addEvent(project, actor_id, message.encode().getBytes());

        aggregator.aggregate(project, message, actor_id, (int) (System.currentTimeMillis() / 1000));
        return "1".getBytes();
      
    }


}

