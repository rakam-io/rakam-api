package org.rakam.analysis;

import org.rakam.server.http.RequestPreprocessor;

import java.lang.reflect.Method;
import java.util.function.Predicate;

public class RequestPreProcessorItem {
    public final Predicate<Method> predicate;
    public final RequestPreprocessor processor;

    public RequestPreProcessorItem(Predicate<Method> predicate, RequestPreprocessor processor) {
        this.predicate = predicate;
        this.processor = processor;
    }
}