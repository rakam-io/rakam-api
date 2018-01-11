package org.rakam.http;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.IgnoreApi;

import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@Path("/")
@IgnoreApi
public class OptionMethodHttpService extends HttpService {
    @OPTIONS
    @Path("/*")
    public static void handle(RakamHttpRequest request) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK);

        response.headers().set(ACCESS_CONTROL_ALLOW_HEADERS, "Origin, X-Requested-With, Content-Type, Accept, master_key, read_key, write_key");
        response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, "_auto_action");
        response.headers().set(ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS, PUT, DELETE");

        request.response(response).end();
    }
}
