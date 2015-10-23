package org.rakam;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import io.netty.channel.nio.NioEventLoopGroup;
import io.swagger.models.Contact;
import io.swagger.models.Info;
import io.swagger.models.License;
import io.swagger.models.Swagger;
import io.swagger.models.Tag;
import io.swagger.models.auth.ApiKeyAuthDefinition;
import io.swagger.models.auth.In;
import org.rakam.collection.event.SecuredForProject;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.config.HttpServerConfig;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpServerBuilder;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

import static org.rakam.collection.event.metastore.Metastore.AccessKeyType.*;

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
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, READ_KEY), method -> test(method, READ_KEY))
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, WRITE_KEY), method -> test(method, WRITE_KEY))
                .addJsonPreprocessor(new ProjectAuthPreprocessor(metastore, MASTER_KEY), method -> test(method, MASTER_KEY))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, MASTER_KEY), method -> test(method, MASTER_KEY))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, WRITE_KEY), method -> test(method, WRITE_KEY))
                .addJsonBeanPreprocessor(new ProjectJsonBeanRequestPreprocessor(metastore, READ_KEY), method -> test(method, READ_KEY))
                .addPreprocessor(new ProjectRawAuthPreprocessor(metastore, READ_KEY), method -> test(method, READ_KEY))
                .addPreprocessor(new ProjectRawAuthPreprocessor(metastore, READ_KEY), method -> test(method, READ_KEY))
                .addPreprocessor(new ProjectRawAuthPreprocessor(metastore, READ_KEY), method -> test(method, READ_KEY))
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
        if(!method.isAnnotationPresent(SecuredForProject.class)) {
            return false;
        }
        final ApiOperation annotation = method.getAnnotation(ApiOperation.class);
        if(annotation != null &&
                !Arrays.stream(annotation.authorizations()).anyMatch(a -> key.getKey().equals(a.value()))) {
            throw new IllegalArgumentException("method %s cannot have @SecuredForProject because " +
                    "it doesn't have appropriate @ApiOperation definition");
        }
        return false;
    }

}
