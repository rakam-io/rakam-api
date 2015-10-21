package org.rakam;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.util.RakamException;

import java.lang.reflect.Method;
import java.util.Arrays;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

class ProjectAuthPreprocessor implements RequestPreprocessor<ObjectNode> {

    private final Metastore metastore;
    private final String keyName;

    public ProjectAuthPreprocessor(Metastore metastore, String keyName) {
        this.metastore = metastore;
        this.keyName = keyName;
    }

    @Override
    public boolean handle(HttpHeaders headers, ObjectNode bodyData) {
        if(!metastore.checkPermission(bodyData.get("project").asText(), Metastore.AccessKeyType.READ_KEY, headers.get(keyName))) {
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
