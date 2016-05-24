package org.rakam.analysis;

import org.rakam.server.http.HttpServerBuilder;

public class CustomParameter {
    public final String parameterName;
    public final HttpServerBuilder.IRequestParameterFactory factory;

    public CustomParameter(String parameterName, HttpServerBuilder.IRequestParameterFactory factory) {
        this.parameterName = parameterName;
        this.factory = factory;
    }
}
