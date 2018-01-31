package org.rakam.http;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.swagger.models.*;
import io.swagger.models.Response;
import io.swagger.models.auth.ApiKeyAuthDefinition;
import io.swagger.models.auth.In;
import io.swagger.models.properties.RefProperty;
import io.swagger.util.PrimitiveType;
import org.apache.avro.generic.GenericRecord;
import org.rakam.Access;
import org.rakam.ServiceStarter;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.CustomParameter;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.RequestPreProcessorItem;
import org.rakam.server.http.*;
import org.rakam.server.http.HttpServerBuilder.IRequestParameterFactory;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.util.*;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

@Singleton
public class WebServiceModule
        extends AbstractModule {
    private final Set<WebSocketService> webSocketServices;
    private final Set<HttpService> httpServices;
    private final HttpServerConfig config;
    private final Set<Tag> tags;
    private final Set<CustomParameter> customParameters;
    private final Set<RequestPreProcessorItem> requestPreProcessorItems;
    private final HttpRequestHandler requestHandler;

    @Inject
    public WebServiceModule(Set<HttpService> httpServices,
                            Set<Tag> tags,
                            Set<CustomParameter> customParameters,
                            Set<RequestPreProcessorItem> requestPreProcessorItems,
                            Set<WebSocketService> webSocketServices,
                            @NotFoundHandler Optional<HttpRequestHandler> requestHandler,
                            HttpServerConfig config) {
        this.httpServices = httpServices;
        this.webSocketServices = webSocketServices;
        this.requestPreProcessorItems = requestPreProcessorItems;
        this.config = config;
        this.tags = tags;
        this.customParameters = customParameters;
        this.requestHandler = requestHandler.orNull();
    }

    @Override
    protected void configure() {
        Info info = new Info()
                .title("Rakam API Documentation")
                .version(ServiceStarter.RAKAM_VERSION)
                .description("An analytics platform API that lets you create your own analytics services.")
                .contact(new Contact().email("contact@rakam.io"))
                .license(new License()
                        .name("Apache License 2.0")
                        .url("http://www.apache.org/licenses/LICENSE-2.0.html"));

        Swagger swagger = new Swagger().info(info)
                .basePath("/")
                .tags(ImmutableList.copyOf(tags))
                .securityDefinition("write_key", new ApiKeyAuthDefinition().in(In.HEADER).name("write_key"))
                .securityDefinition("read_key", new ApiKeyAuthDefinition().in(In.HEADER).name("read_key"))
                .securityDefinition("master_key", new ApiKeyAuthDefinition().in(In.HEADER).name("master_key"));

        EventLoopGroup eventExecutors;
        if (Epoll.isAvailable()) {
            eventExecutors = new EpollEventLoopGroup();
        } else {
            eventExecutors = new NioEventLoopGroup();
        }

        HttpServerBuilder httpServer = new HttpServerBuilder()
                .setHttpServices(httpServices)
                .setWebsocketServices(webSocketServices)
                .setSwagger(swagger)
                .setMaximumBody(Runtime.getRuntime().maxMemory() / 10)
                .setEventLoopGroup(eventExecutors)
                .setSwaggerOperationProcessor((method, operation) -> {
                    ApiOperation annotation = method.getAnnotation(ApiOperation.class);
                    if (annotation != null && annotation.authorizations() != null && annotation.authorizations().length > 0) {
                        String value = annotation.authorizations()[0].value();
                        if (value != null && !value.isEmpty()) {
                            operation.response(FORBIDDEN.code(), new Response()
                                    .schema(new RefProperty("ErrorMessage"))
                                    .description(value + " is invalid"));
                        }
                    }
                })
                .setMapper(JsonHelper.getMapper())
                .setProxyProtocol(config.getProxyProtocol())
                .setExceptionHandler((request, ex) -> {
                    if (ex instanceof RakamException) {
                        LogUtil.logException(request, (RakamException) ex);
                    }
                    if (!(ex instanceof HttpRequestException)) {
                        LogUtil.logException(request, ex);
                    }
                })
                .setOverridenMappings(ImmutableMap.of(GenericRecord.class, PrimitiveType.OBJECT, ZoneId.class, PrimitiveType.STRING))
                .addPostProcessor(response -> response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true"),
                        method -> method.isAnnotationPresent(AllowCookie.class));

        requestPreProcessorItems.forEach(i -> httpServer.addJsonPreprocessor(i.processor, i.predicate));

        ImmutableMap.Builder<String, IRequestParameterFactory> builder = ImmutableMap.<String, IRequestParameterFactory>builder();

        for (CustomParameter customParameter : customParameters) {
            builder.put(customParameter.parameterName, customParameter.factory);
        }

        httpServer.setCustomRequestParameters(builder.build());
        HttpServer build = httpServer.build();

        if (requestHandler != null) {
            build.setNotFoundHandler(requestHandler);
        }

        HostAndPort address = config.getAddress();
        try {
            build.bind(address.getHostText(), address.getPort());
        } catch (InterruptedException e) {
            addError(e);
            return;
        }

        binder().bind(HttpServer.class).toInstance(build);
    }

    public static class ProjectPermissionIRequestParameter
            implements IRequestParameter {

        private final ApiKeyService.AccessKeyType type;
        private final ApiKeyService apiKeyService;

        public ProjectPermissionIRequestParameter(ApiKeyService apiKeyService, Method method) {
            final ApiOperation annotation = method.getAnnotation(ApiOperation.class);
            Authorization[] authorizations = annotation == null ?
                    new Authorization[0] :
                    Arrays.stream(annotation.authorizations()).filter(auth -> !auth.value().equals("")).toArray(value -> new Authorization[value]);

            if (authorizations.length == 0) {
                throw new IllegalStateException(method.toGenericString() + ": The permission check component requires endpoints to have authorizations definition in @ApiOperation. " +
                        "Use @IgnorePermissionCheck to bypass security check in method " + method.toString());
            }

            if (annotation != null && !annotation.consumes().isEmpty() && !annotation.consumes().equals("application/json")) {
                throw new IllegalStateException("The permission check component requires endpoint to consume application/json. " +
                        "Use @IgnorePermissionCheck to bypass security check in method " + method.toString());
            }
            Api clazzOperation = method.getDeclaringClass().getAnnotation(Api.class);
            if (authorizations.length == 0 && (clazzOperation == null || clazzOperation.authorizations().length == 0)) {
                throw new IllegalArgumentException(String.format("Authorization for method %s is not defined. " +
                        "You must use @IgnorePermissionCheck if the endpoint doesn't need permission check", method.toString()));
            }

            if (authorizations.length != 1) {
                throw new IllegalArgumentException();
            }

            type = ApiKeyService.AccessKeyType.fromKey(authorizations[0].value());
            this.apiKeyService = apiKeyService;
        }

        @Override
        public Object extract(ObjectNode node, RakamHttpRequest request) {
            String apiKey = request.headers().get(type.getKey());
            if (apiKey == null) {
                List<String> apiKeyList = request.params().get(type.getKey());
                if (apiKeyList != null && !apiKeyList.isEmpty()) {
                    apiKey = apiKeyList.get(0);
                } else {
                    throw new RakamException(type.getKey() + " header or " +
                            "query parameter is missing.", FORBIDDEN);
                }
            }

            Access access;
            if (apiKey.length() > 64) {
                int apiKeyId = -1;

                for (int i = 0; i < apiKey.length(); i++) {
                    if (!Character.isDigit(apiKey.charAt(i))) {
                        if (i == 0) {
                            throw new RakamException("Invalid API key format.", FORBIDDEN);
                        }
                    }
                    apiKeyId = Integer.parseInt(apiKey.substring(0, i));
                }

                if (apiKeyId == -1) {
                    throw new RakamException("Invalid API key format.", FORBIDDEN);
                }
                ApiKeyService.Key projectKey = apiKeyService.getProjectKey(apiKeyId, type);
                String data = CryptUtil.decryptAES(apiKey.substring(apiKeyId), projectKey.key);
                access = JsonHelper.read(data, Access.class);
            } else {
                access = null;
            }

            String project = apiKeyService.getProjectOfApiKey(apiKey, type);
            return new RequestContext(project.toLowerCase(Locale.ENGLISH), apiKey, access);
        }
    }
}
