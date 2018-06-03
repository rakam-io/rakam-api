package org.rakam.analysis.webhook;

import io.airlift.configuration.Config;
import org.rakam.util.JsonHelper;

import javax.validation.constraints.NotNull;
import java.util.Map;

public class WebhookConfig {
    private String url;
    private Map<String, String> headers;

    @NotNull
    public String getUrl() {
        return url;
    }

    @Config("collection.webhook.url")
    public WebhookConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Config("collection.webhook.headers")
    public WebhookConfig setHeaders(String headers) {
        this.headers = JsonHelper.read(headers, Map.class);
        return this;
    }
}
