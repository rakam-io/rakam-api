package org.rakam.http;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.util.RakamException;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.util.ValidationUtil.checkProject;

class ProjectAuthPreprocessor implements RequestPreprocessor<ObjectNode> {

    private final Metastore metastore;
    private final Metastore.AccessKeyType key;

    public ProjectAuthPreprocessor(Metastore metastore, Metastore.AccessKeyType key) {
        this.metastore = metastore;
        this.key = key;
    }

    @Override
    public void handle(HttpHeaders headers, ObjectNode bodyData) {
        String project = bodyData.get("project").asText();
        checkProject(project);
        if(!metastore.checkPermission(project, key, headers.get("api_key"))) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
    }
}
