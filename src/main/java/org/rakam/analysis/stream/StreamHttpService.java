package org.rakam.analysis.stream;

import com.google.inject.Inject;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:49.
 */
@Path("/stream")
public class StreamHttpService implements HttpService {
    private final EventStream stream;

    @Inject
    public StreamHttpService(EventStream stream) {
        this.stream = stream;
    }

    @GET
    @Path("/subscribe")
    public void subscribe(RakamHttpRequest request) {
        //stream.subscribe();
    }
}
