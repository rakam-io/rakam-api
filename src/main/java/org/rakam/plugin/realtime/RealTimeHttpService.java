package org.rakam.plugin.realtime;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 14:30.
 */
@Path("/realtime")
public class RealTimeHttpService implements HttpService {
    @GET
    @Path("/")
    public static void getWidget(RakamHttpRequest request) {
        request.response("hey").end();
    }
}
