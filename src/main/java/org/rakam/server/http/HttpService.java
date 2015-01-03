package org.rakam.server.http;

import org.rakam.server.RouteMatcher;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/10/14 14:35.
 */
public interface HttpService {
    String getEndPoint();

    void register(RouteMatcher.MicroRouteMatcher routeMatcher);
}
