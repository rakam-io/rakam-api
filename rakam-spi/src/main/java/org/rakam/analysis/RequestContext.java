package org.rakam.analysis;

public class RequestContext {
    public final String project;
    public final String apiKey;

    public RequestContext(String project, String apiKey) {
        this.project = project;
        this.apiKey = apiKey;
    }
}
