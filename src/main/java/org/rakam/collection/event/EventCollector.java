package org.rakam.collection.event;

import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.plugin.CollectionMapperPlugin;
import org.rakam.util.json.JsonObject;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 21:48.
 */
public class EventCollector {
    public final ExecutorService executor = new ThreadPoolExecutor(35, 50, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());

    private EventDatabase databaseAdapter;
    List<CollectionMapperPlugin> mappers = new LinkedList();
    private EventAggregator eventAggregator;

    public EventCollector(Injector injector) {
        try {
            AnalysisRuleDatabase ruleDatabase = injector.getInstance(AnalysisRuleDatabase.class);
            databaseAdapter = injector.getInstance(EventDatabase.class);
            eventAggregator = new EventAggregator(injector, ruleDatabase);

            List<Binding<CollectionMapperPlugin>> bindingsByType = injector
                    .findBindingsByType(new TypeLiteral<CollectionMapperPlugin>() {});
            mappers.addAll(bindingsByType.stream().map(mapper -> mapper.getProvider().get()).collect(Collectors.toList()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean submitEvent(JsonObject json) {
        String project = json.getString("project");
        if (project == null) {
            return false;
        }
        String actor_id = json.getString("user");
        String name = json.getString("name");
        JsonObject properties = json.getJsonObject("properties");

        if (mappers.stream().anyMatch(mapper -> !mapper.map(json)))
            return true;

        databaseAdapter.addEvent(project, name, actor_id, properties);
        eventAggregator.aggregate(project, json, actor_id, null);
        return true;
    }

    public void submitActor(JsonObject json) {
        executor.submit(() -> {
            String tracker = json.getString("tracker");
            String actor_id = json.getString("user");

            JsonObject attributes = json.getJsonObject("attrs");
//            databaseAdapter.addPropertyToActor(tracker, actor_id, attributes.getMap());
        });
    }


}
