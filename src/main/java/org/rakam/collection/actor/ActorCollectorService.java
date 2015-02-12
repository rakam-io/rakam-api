package org.rakam.collection.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.util.JsonHelper;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
@Path("/actor")
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

    @POST
    @Path("/collect")
    public void collect(RakamHttpRequest request) {
        final ObjectNode json = JsonHelper.generate(request.params());
        request.response(handle(json) ? "1" : "0");
    }
}
