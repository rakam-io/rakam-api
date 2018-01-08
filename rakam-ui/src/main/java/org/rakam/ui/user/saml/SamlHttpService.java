package org.rakam.ui.user.saml;

import com.coveo.saml.SamlClient;
import com.coveo.saml.SamlException;
import com.coveo.saml.SamlResponse;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.Response;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.user.WebUser;
import org.rakam.ui.user.WebUserService;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.cookie.ServerCookieEncoder.STRICT;
import static org.rakam.ui.user.WebUserHttpService.getLoginResponseForUser;

@Path("/saml")
@Api(value = "/saml", nickname = "saml", description = "SAML operations", tags = "saml")
public class SamlHttpService
        extends HttpService {
    private final SamlClient client;
    private final WebUserService service;
    private final String secretKey;
    private final SamlConfig config;

    @Inject
    public SamlHttpService(EncryptionConfig encryptionConfig, WebUserService service, SamlConfig config) {
        this.service = service;
        this.config = config;
        this.secretKey = encryptionConfig.getSecretKey();
        try {
            client = SamlClient.fromMetadata(null, "http://some/url/that/processes/assertions", new StringReader(config.getSamlMetadata()));
        } catch (SamlException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Path("/login")
    @GET
    public void redirect(RakamHttpRequest request) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, FOUND, Unpooled.wrappedBuffer(new byte[]{}));
        response.headers().add("Location", client.getIdentityProviderUrl());

        request.response(response).end();
    }

    @Path("/check")
    @GET
    public void check(RakamHttpRequest request) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, FOUND, Unpooled.wrappedBuffer(new byte[]{}));
        response.headers().add("Location", client.getIdentityProviderUrl());

        request.response(response).end();
    }

    @Path("/callback")
    @JsonRequest
    public void callback(RakamHttpRequest request) {
        request.bodyHandler(inputStream -> {
            String post;
            try {
                post = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            QueryStringDecoder decoder = new QueryStringDecoder("?" + post);
            List<String> samlResponseParam = decoder.parameters().get("SAMLResponse");

            String data;
            if (samlResponseParam == null) {
                data = "";
            } else {
                data = samlResponseParam.get(0);
            }

            SamlResponse samlResponse;
            try {
                samlResponse = client.decodeAndValidateSamlResponse(data);
            } catch (SamlException e) {
                throw new RakamException(e.getMessage(), BAD_GATEWAY);
            }

            Optional<WebUser> userByEmail = service.getUserByEmail(samlResponse.getNameID());
            WebUser user = userByEmail.orElseGet(() ->
                    service.createUser(samlResponse.getNameID(),
                            null, null,
                            null,
                            null,
                            null, true));

            Response loginResponseForUser = getLoginResponseForUser(secretKey, user, config.getSamlCookieTtl());

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, FOUND, Unpooled.wrappedBuffer(new byte[]{}));
            response.headers().add("Location", "/");
            response.headers().add(SET_COOKIE, STRICT.encode(loginResponseForUser.getCookies()));

            request.response(response).end();
        });
    }
}
