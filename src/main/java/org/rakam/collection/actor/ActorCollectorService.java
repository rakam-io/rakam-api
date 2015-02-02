package org.rakam.collection.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.rakam.server.RouteMatcher;
import org.rakam.server.http.HttpService;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.util.JsonHelper;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
public class ActorCollectorService implements HttpService {

    private final ActorCacheAdapter actorCache;

    @Inject
    public ActorCollectorService(ActorCacheAdapter actorCache) {
        this.actorCache = actorCache;
    }

    public boolean handle(ObjectNode json) {
        String project = json.get("project").asText();
        String actorId = json.get("id").asText();

        if(project==null || actorId==null) {
            return false;
        }

        JsonNode properties = json.get("properties");
        if(properties!=null) {
            actorCache.setActorProperties(project, actorId, properties);
        }

//        databaseAdapter.createActor(project, actorId, properties);
        return true;
    }

    @Override
    public String getEndPoint() {
        return "/actor";
    }

    @Override
    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {
        routeMatcher.add("/collect", HttpMethod.GET, (request) -> {
            final ObjectNode json = JsonHelper.generate(request.params());
            request.response(handle(json) ? "1" : "0");
        });
    }
}
