package org.rakam.collection.event;

import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.rakam.analysis.AnalysisRuleMap;
import org.rakam.database.DatabaseAdapter;
import org.rakam.plugin.CollectionMapperPlugin;
import org.rakam.util.json.JsonObject;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 21:48.
 */
public class EventCollector {
    public final ExecutorService executor = new ThreadPoolExecutor(35, 50, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());

    private DatabaseAdapter databaseAdapter;
    List<CollectionMapperPlugin> mappers = new LinkedList();
    final private EventAggregator eventAggregator;

    public EventCollector(Injector injector, AnalysisRuleMap analysisRuleMap) {
        databaseAdapter = injector.getInstance(DatabaseAdapter.class);
        eventAggregator = new EventAggregator(injector, analysisRuleMap);

        List<Binding<CollectionMapperPlugin>> bindingsByType = injector
                .findBindingsByType(new TypeLiteral<CollectionMapperPlugin>() {});
        mappers.addAll(bindingsByType.stream().map(mapper -> mapper.getProvider().get()).collect(Collectors.toList()));

        final PeriodicCollector props = new PeriodicCollector(injector);
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            try {
                props.process(analysisRuleMap.entrySet());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    public boolean submitEvent(JsonObject json) {
        String project = json.getString("_tracker");
        if (project == null) {
            return false;
        }
        String actor_id = json.getString("_user");

        if (mappers.stream().anyMatch(mapper -> !mapper.map(json)))
            return true;

        databaseAdapter.addEventAsync(project, actor_id, json);
        eventAggregator.aggregate(project, json, actor_id, null);
        return true;
    }

    public void submitActor(JsonObject json) {
        executor.submit(() -> {
            String tracker = json.getString("tracker");
            String actor_id = json.getString("user");

            JsonObject attributes = json.getObject("attrs");
            databaseAdapter.addPropertyToActor(tracker, actor_id, attributes.toMap());
        });
    }


}
