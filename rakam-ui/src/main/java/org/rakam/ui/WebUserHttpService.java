package org.rakam.ui;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.ws.rs.Path;
import java.util.Optional;

@Path("/ui/user")
public class WebUserHttpService extends HttpService {

    private final WebUserService service;

    @Inject
    public WebUserHttpService(WebUserService service) {
        this.service = service;
    }

    @JsonRequest
    @Path("/register")
    public JsonResponse register(@ApiParam(name="email") String email,
                          @ApiParam(name="name") String name,
                          @ApiParam(name="password") String password) {
        // TODO: implement captcha https://github.com/VividCortex/angular-recaptcha https://developers.google.com/recaptcha/docs/verify
        // keep a counter for ip in local nodes and use stickiness feature of load balancer
        service.createUser(email, name, password);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/login")
    public WebUser login(@ApiParam(name="email") String email,
                               @ApiParam(name="password") String password) {
        final Optional<WebUser> user = service.getUser(email, password);

        if(user.isPresent()) {
            return user.get();
        }

        throw new RakamException("Account couldn't found.", HttpResponseStatus.NOT_FOUND);
    }
}
