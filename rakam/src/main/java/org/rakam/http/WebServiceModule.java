package org.rakam.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.swagger.models.Contact;
import io.swagger.models.Info;
import io.swagger.models.License;
import io.swagger.models.Swagger;
import io.swagger.models.Tag;
import io.swagger.models.auth.ApiKeyAuthDefinition;
import io.swagger.models.auth.In;
import io.swagger.util.PrimitiveType;
import org.apache.avro.generic.GenericRecord;
import org.rakam.ServiceStarter;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpServerBuilder;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.util.Os;
import org.rakam.util.AllowCookie;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static org.rakam.analysis.metadata.Metastore.AccessKeyType.*;

@Singleton
public class WebServiceModule extends AbstractModule {
    private final Set<WebSocketService> webSocketServices;
    private final Set<HttpService> httpServices;
    private final HttpServerConfig config;
    private final Set<Tag> tags;
    private final Metastore metastore;

    @Inject
    public WebServiceModule(Set<HttpService> httpServices, Set<Tag> tags, Metastore metastore, Set<WebSocketService> webSocketServices, HttpServerConfig config) {
        this.httpServices = httpServices;
        this.webSocketServices = webSocketServices;
        this.config = config;
        this.tags = tags;
        this.metastore = metastore;
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
                .host("app.getrakam.com")
                .basePath("/")
                .tags(ImmutableList.copyOf(tags))
                .securityDefinition("write_key", new ApiKeyAuthDefinition().in(In.HEADER).name("write_key"))
                .securityDefinition("read_key", new ApiKeyAuthDefinition().in(In.HEADER).name("read_key"))
                .securityDefinition("master_key", new ApiKeyAuthDefinition().in(In.HEADER).name("master_key"));

        EventLoopGroup eventExecutors;
        if (Os.supportsEpoll()) {
            eventExecutors = new EpollEventLoopGroup();
        } else {
            eventExecutors = new NioEventLoopGroup();
        }

        HttpServer httpServer =  new HttpServerBuilder()
                .setHttpServices(httpServices)
                .setWebsockerServices(webSocketServices)
                .setSwagger(swagger)
                .setEventLoopGroup(eventExecutors)
                .setMapper(JsonHelper.getMapper())
                .setDebugMode(config.getDebug())
                .setProxyProtocol(config.getProxyProtocol())
                .setOverridenMappings(ImmutableMap.of(GenericRecord.class, PrimitiveType.OBJECT))
                .addPostProcessor(response -> response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true"), method -> method.isAnnotationPresent(AllowCookie.class))
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, READ_KEY), method -> test(method, READ_KEY))
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, WRITE_KEY), method -> test(method, WRITE_KEY))
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, MASTER_KEY), method -> test(method, MASTER_KEY))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, MASTER_KEY), method -> ProjectJsonBeanRequestPreprocessor.test(method, MASTER_KEY))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, WRITE_KEY), method -> ProjectJsonBeanRequestPreprocessor.test(method, WRITE_KEY))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, READ_KEY), method -> ProjectJsonBeanRequestPreprocessor.test(method, READ_KEY))
                .addPreprocessor(new ProjectRawAuthPreprocessor(metastore, READ_KEY), method -> test(method, READ_KEY))
                .addPreprocessor(new ProjectRawAuthPreprocessor(metastore, WRITE_KEY), method -> test(method, WRITE_KEY))
                .addPreprocessor(new ProjectRawAuthPreprocessor(metastore, MASTER_KEY), method -> test(method, MASTER_KEY))
                .build();

        HostAndPort address = config.getAddress();
        try {
            httpServer.bind(address.getHostText(), address.getPort());
        } catch (InterruptedException e) {
            addError(e);
            return;
        }

        binder().bind(HttpServer.class).toInstance(httpServer);
    }


    public static boolean test(Method method, Metastore.AccessKeyType key) {
        if(method.isAnnotationPresent(IgnorePermissionCheck.class)) {
            return false;
        }
        final ApiOperation annotation = method.getAnnotation(ApiOperation.class);
        Authorization[] authorizations = annotation == null ? new Authorization[0] : annotation.authorizations();

        if(authorizations.length == 0) {
            throw new IllegalStateException(method.toGenericString()+": The permission check component requires endpoints to have authorizations definition in @ApiOperation. " +
                    "Use @IgnorePermissionCheck to bypass security check in method " + method.toString());
        }

        if(annotation != null && !annotation.consumes().isEmpty() && !annotation.consumes().equals("application/json")) {
            throw new IllegalStateException("The permission check component requires endpoint to consume application/json. " +
                    "Use @IgnorePermissionCheck to bypass security check in method " + method.toString());
        }
        Api clazzOperation = method.getDeclaringClass().getAnnotation(Api.class);
        if(authorizations.length == 0 && (clazzOperation == null || clazzOperation.authorizations().length == 0)) {
            throw new IllegalArgumentException(String.format("Authorization for method %s is not defined. " +
                    "You must use @IgnorePermissionCheck if the endpoint doesn't need permission check", method.toString()));
        }

        return Arrays.stream(authorizations).anyMatch(a -> key.getKey().equals(a.value())) ||
                (clazzOperation != null && Arrays.stream(clazzOperation.authorizations()).anyMatch(a -> key.getKey().equals(a.value())));
    }

}
