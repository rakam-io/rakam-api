package org.rakam;

import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.util.RakamException;

import java.lang.reflect.Method;
import java.util.Arrays;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

class ProjectRawAuthPreprocessor implements RequestPreprocessor<RakamHttpRequest> {

    private final Metastore metastore;
    private final Metastore.AccessKeyType key;

    public ProjectRawAuthPreprocessor(Metastore metastore, Metastore.AccessKeyType key) {
        this.metastore = metastore;
        this.key = key;
    }

    @Override
    public boolean handle(HttpHeaders headers, RakamHttpRequest bodyData) {
        if(!metastore.checkPermission(headers.get("project"), key, headers.get("api_key"))) {
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
