package org.rakam.http;

import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.util.RakamException;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.util.ValidationUtil.checkProject;

class ProjectRawAuthPreprocessor implements RequestPreprocessor<RakamHttpRequest> {

    private final Metastore metastore;
    private final Metastore.AccessKeyType key;

    public ProjectRawAuthPreprocessor(Metastore metastore, Metastore.AccessKeyType key) {
        this.metastore = metastore;
        this.key = key;
    }

    @Override
    public void handle(HttpHeaders headers, RakamHttpRequest request) {
        String project = headers.get("project");
        String api_key = headers.get("api_key");
        checkProject(project);
        if(project == null || api_key == null || !metastore.checkPermission(project, key, api_key))
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
    }
}
