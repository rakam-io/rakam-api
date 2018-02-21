package org.rakam.analysis;

import org.rakam.Access;

public class RequestContext {
    public final String project;
    public final String apiKey;
    public final Access access;

    public RequestContext(String project) {
        this(project, null, null);
    }

    public RequestContext(String project, String apiKey) {
        this(project, apiKey, null);
    }

    public RequestContext(String project, String apiKey, Access access) {
        this.project = project;
        this.apiKey = apiKey;
        this.access = access;
    }
}
