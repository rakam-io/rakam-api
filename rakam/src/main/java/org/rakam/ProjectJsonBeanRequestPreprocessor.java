package org.rakam;

import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.util.RakamException;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

class ProjectJsonBeanRequestPreprocessor implements RequestPreprocessor<Object> {
    private final Metastore metastore;
    private final Metastore.AccessKeyType key;

    public ProjectJsonBeanRequestPreprocessor(Metastore metastore,  Metastore.AccessKeyType key) {
        this.metastore = metastore;
        this.key = key;
    }

    @Override
    public boolean handle(HttpHeaders headers, Object bodyData) {
        if(!metastore.checkPermission(((ProjectItem) bodyData).project(), key, headers.get("api_key"))) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
        return true;
    }
}
