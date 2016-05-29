package org.rakam.http;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.swagger.models.Contact;
import io.swagger.models.Info;
import io.swagger.models.License;
import io.swagger.models.Response;
import io.swagger.models.Swagger;
import io.swagger.models.Tag;
import io.swagger.models.auth.ApiKeyAuthDefinition;
import io.swagger.models.auth.In;
import io.swagger.models.properties.RefProperty;
import io.swagger.util.PrimitiveType;
import org.apache.avro.generic.GenericRecord;
import org.rakam.ServiceStarter;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.CustomParameter;
import org.rakam.analysis.RequestPreProcessorItem;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpServerBuilder;
import org.rakam.server.http.HttpServerBuilder.IRequestParameterFactory;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.IRequestParameter;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.WebSocketService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.util.AllowCookie;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.SentryUtil;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

@Singleton
public class WebServiceModule extends AbstractModule {
    private final Set<WebSocketService> webSocketServices;
    private final Set<HttpService> httpServices;
    private final HttpServerConfig config;
    private final Set<Tag> tags;
    private final Set<CustomParameter> customParameters;
    private final Set<RequestPreProcessorItem> requestPreProcessorItems;

    @Inject
    public WebServiceModule(Set<HttpService> httpServices,
                            Set<Tag> tags,
                            Set<CustomParameter> customParameters,
                            Set<RequestPreProcessorItem> requestPreProcessorItems,
                            Set<WebSocketService> webSocketServices,
                            HttpServerConfig config) {
        this.httpServices = httpServices;
        this.webSocketServices = webSocketServices;
        this.requestPreProcessorItems = requestPreProcessorItems;
        this.config = config;
        this.tags = tags;
        this.customParameters = customParameters;
    }

    @Override
    protected void configure() {
        Info info = new Info()
                .title("Rakam API Documentation")
                .version(ServiceStarter.RAKAM_VERSION)
                .description("An analytics platform API that lets you create your own analytics services.")
                .contact(new Contact().email("contact@rakam.com"))
                .license(new License()
                        .name("Apache License 2.0")
                        .url("http://www.apache.org/licenses/LICENSE-2.0.html"));

        Swagger swagger = new Swagger().info(info)
                .host("app.rakam.io")
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
                .setWebsockerServices(webSocketServices)
                .setSwagger(swagger)
                .setEventLoopGroup(eventExecutors)
                .setSwaggerOperationProcessor((method, operation) -> {
                    ApiOperation annotation = method.getAnnotation(ApiOperation.class);
                    if (annotation != null && annotation.authorizations() != null && annotation.authorizations().length > 0) {
                        operation.response(FORBIDDEN.code(), new Response()
                                .schema(new RefProperty("ErrorMessage"))
                                .description("Api key is invalid"));
                    }
                })
                .setMapper(JsonHelper.getMapper())
                .setDebugMode(config.getDebug())
                .setProxyProtocol(config.getProxyProtocol())
                .setExceptionHandler((request, ex) -> {
                    if (ex instanceof RakamException) {
                        SentryUtil.logException(request, (RakamException) ex);
                    }
                })
                .setOverridenMappings(ImmutableMap.of(GenericRecord.class, PrimitiveType.OBJECT))
                .addPostProcessor(response -> response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true"), method -> method.isAnnotationPresent(AllowCookie.class));

        requestPreProcessorItems.forEach(i -> httpServer.addJsonPreprocessor(i.processor, i.predicate));

        ImmutableMap.Builder<String, IRequestParameterFactory> builder = ImmutableMap.<String, IRequestParameterFactory>builder();

        for (CustomParameter customParameter : customParameters) {
            builder.put(customParameter.parameterName, customParameter.factory);
        }

        httpServer.setCustomRequestParameters(builder.build());
        HttpServer build = httpServer.build();

        HostAndPort address = config.getAddress();
        try {
            build.bind(address.getHostText(), address.getPort());
        } catch (InterruptedException e) {
            addError(e);
            return;
        }

        binder().bind(HttpServer.class).toInstance(build);
    }

    public static class ProjectPermissionParameterFactory implements IRequestParameterFactory {

        private final ApiKeyService service;

        public ProjectPermissionParameterFactory(ApiKeyService service) {
            this.service = service;
        }

        @Override
        public IRequestParameter create(Method method) {
            return new ProjectPermissionIRequestParameter(service, method);
        }
    }

    private static class ProjectPermissionIRequestParameter implements IRequestParameter {

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
            String api_key = request.headers().get("api_key");
            if (api_key == null) {
                throw new RakamException("api_key header parameter is missing.", FORBIDDEN);
            }
            return apiKeyService.getProjectOfApiKey(api_key, type);
        }
    }

}
