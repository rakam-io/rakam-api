package org.rakam.plugin.realtime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.ringmap.RingMap;
import org.rakam.model.Event;
import org.rakam.plugin.EventProcessor;
import org.rakam.util.JsonHelper;

import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 13:41.
 */
public class RealTimeEventProcessor implements EventProcessor {
    RingMap<String, Map<String, Object>> db;

    @Inject
    public RealTimeEventProcessor(Cluster cluster) {
        db = cluster.createOrGetService("realtime",
                bus -> new RingMap<>(bus, (first, second) -> {
                    first.putAll(second);
                    return first;
                }, 1));
    }

    @Override
    public void process(Event event) {
        ObjectNode a = JsonHelper.jsonObject();
        a.

        db.merge(event.user, event.properties, )
        event.user;
    }
}
