package org.rakam.plugin.realtime;

import io.netty.handler.codec.http.HttpMethod;
import org.rakam.server.RouteMatcher;
import org.rakam.server.http.HttpRequestHandler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.Path;
import org.rakam.server.http.RakamHttpRequest;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 14:30.
 */
@Path("/realtime")
public class RealTimeHttpService implements HttpService {

    @Override
    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {
        routeMatcher
                .add("/add", HttpMethod.GET, new HttpRequestHandler() {
                    @Override
                    public void handle(RakamHttpRequest request) {

                    }
                });
    }
}
