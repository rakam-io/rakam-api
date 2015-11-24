package org.rakam;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.util.RakamException;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

class ProjectAuthPreprocessor implements RequestPreprocessor<ObjectNode> {

    private final Metastore metastore;
    private final Metastore.AccessKeyType key;

    public ProjectAuthPreprocessor(Metastore metastore, Metastore.AccessKeyType key) {
        this.metastore = metastore;
        this.key = key;
    }

    @Override
    public void handle(HttpHeaders headers, ObjectNode bodyData) {
        if(!metastore.checkPermission(bodyData.get("project").asText(), key, headers.get("api_key"))) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
    }
}
