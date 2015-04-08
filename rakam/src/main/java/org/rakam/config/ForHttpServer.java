package org.rakam.config;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 22:47.
 */
@BindingAnnotation
@Retention(RetentionPolicy.RUNTIME)
public @interface ForHttpServer {
}
