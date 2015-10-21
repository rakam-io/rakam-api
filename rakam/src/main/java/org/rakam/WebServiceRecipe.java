package org.rakam;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.swagger.models.Contact;
import io.swagger.models.Info;
import io.swagger.models.License;
import io.swagger.models.Swagger;
import io.swagger.models.Tag;
import io.swagger.models.auth.ApiKeyAuthDefinition;
import io.swagger.models.auth.In;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.config.HttpServerConfig;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpServerBuilder;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.server.http.WebSocketService;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;

import static org.rakam.collection.event.metastore.Metastore.AccessKeyType.MASTER_KEY;
import static org.rakam.collection.event.metastore.Metastore.AccessKeyType.READ_KEY;
import static org.rakam.collection.event.metastore.Metastore.AccessKeyType.WRITE_KEY;

@Singleton
public class WebServiceRecipe extends AbstractModule {

    private final Set<WebSocketService> webSocketServices;
    private final Set<HttpService> httpServices;
    private final HttpServerConfig config;
    private final Set<Tag> tags;
    private final Metastore metastore;

    @Inject
    public WebServiceRecipe(Set<HttpService> httpServices, Set<Tag> tags, Metastore metastore, Set<WebSocketService> webSocketServices, HttpServerConfig config) {
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
                .version("1.0")
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

        NioEventLoopGroup eventExecutors = new NioEventLoopGroup();

        HttpServer httpServer =  new HttpServerBuilder()
                .setHttpServices(httpServices)
                .setWebsockerServices(webSocketServices)
                .setSwagger(swagger)
                .setEventLoopGroup(eventExecutors)
                .setMapper(JsonHelper.getMapper())
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, READ_KEY.getKey()), method -> ProjectAuthPreprocessor.test(method, READ_KEY.getKey()))
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, WRITE_KEY.getKey()), method -> ProjectAuthPreprocessor.test(method, WRITE_KEY.getKey()))
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, MASTER_KEY.getKey()), method -> ProjectAuthPreprocessor.test(method, MASTER_KEY.getKey()))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, MASTER_KEY.getKey()),
                        method -> ProjectJsonBeanRequestPreprocessor.test(method, MASTER_KEY.getKey()))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, WRITE_KEY.getKey()),
                        method -> ProjectJsonBeanRequestPreprocessor.test(method, WRITE_KEY.getKey()))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, READ_KEY.getKey()),
                        method -> ProjectJsonBeanRequestPreprocessor.test(method, READ_KEY.getKey()))

                .addPreprocessor(new RequestPreprocessor<RakamHttpRequest>() {
                    @Override
                    public boolean handle(HttpHeaders headers, RakamHttpRequest bodyData) {
                        return false;
                    }
                }, new Predicate<Method>() {
                    @Override
                    public boolean test(Method method) {
                        final ApiOperation annotation = method.getAnnotation(ApiOperation.class);
                        if(annotation != null) {
                            return Arrays.stream(annotation.authorizations()).anyMatch(a -> keyName.equals(a.value()));
                        }
                        return false;
                    }
                })

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

}
