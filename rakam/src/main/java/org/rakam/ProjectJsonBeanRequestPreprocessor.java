package org.rakam;

import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.util.RakamException;

import java.lang.reflect.Method;
import java.util.Arrays;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

class ProjectJsonBeanRequestPreprocessor implements RequestPreprocessor<Object> {
    private final Metastore metastore;
    private final String keyName;

    public ProjectJsonBeanRequestPreprocessor(Metastore metastore, String keyName) {
        this.metastore = metastore;
        this.keyName = keyName;
    }

    @Override
    public boolean handle(HttpHeaders headers, Object bodyData) {
        if(!metastore.checkPermission(((ProjectItem) bodyData).project(), Metastore.AccessKeyType.READ_KEY, headers.get(keyName))) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
        return true;
    }

    public static boolean test(Method method, String keyName) {
        final ApiOperation annotation = method.getAnnotation(ApiOperation.class);
        if(annotation != null) {
            return Arrays.stream(annotation.authorizations()).anyMatch(a -> keyName.equals(a.value()));
        }
        return false;
    }
}
