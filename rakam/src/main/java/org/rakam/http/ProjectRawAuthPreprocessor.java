package org.rakam.http;

import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ApiKeyService.AccessKeyType;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RequestPreprocessor;
import org.rakam.util.RakamException;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.util.ValidationUtil.checkProject;

class ProjectRawAuthPreprocessor implements RequestPreprocessor<RakamHttpRequest> {

    private final ApiKeyService apiKeyService;
    private final AccessKeyType key;

    public ProjectRawAuthPreprocessor(ApiKeyService apiKeyService, AccessKeyType key) {
        this.apiKeyService = apiKeyService;
        this.key = key;
    }

    @Override
    public void handle(HttpHeaders headers, RakamHttpRequest request) {
        String project = headers.get("project");
        String api_key = headers.get("api_key");
        checkProject(project);
        if(project == null || api_key == null || !apiKeyService.checkPermission(project, key, api_key))
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
    }
}
