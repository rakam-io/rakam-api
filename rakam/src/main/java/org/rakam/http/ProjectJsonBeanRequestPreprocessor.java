package org.rakam.http;

import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.util.RakamException;

import java.lang.reflect.Method;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.util.ValidationUtil.checkProject;

class ProjectJsonBeanRequestPreprocessor implements RequestPreprocessor<Object> {
    private final Metastore metastore;
    private final Metastore.AccessKeyType key;

    public ProjectJsonBeanRequestPreprocessor(Metastore metastore,  Metastore.AccessKeyType key) {
        this.metastore = metastore;
        this.key = key;
    }

    @Override
    public void handle(HttpHeaders headers, Object bodyData) {
        String api_key = headers.get("api_key");
        String project = ((ProjectItem) bodyData).project();
        checkProject(project);
        if(api_key == null || !metastore.checkPermission(project, key, api_key)) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
    }

    public static boolean test(Method method, Metastore.AccessKeyType key) {
        if(!ProjectItem.class.isAssignableFrom((Class) method.getParameters()[0].getParameterizedType())) {
            throw new IllegalStateException("Beans used by @BodyParam must implement org.rakam.ProjectItem interface: "+method.toString());
        }
        return WebServiceModule.test(method, key);
    }
}
