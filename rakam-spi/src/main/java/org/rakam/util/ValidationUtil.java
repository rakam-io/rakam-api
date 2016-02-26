package org.rakam.util;

import io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;


public final class ValidationUtil {

    private ValidationUtil() throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static void checkProject(String project) {
        checkArgument(project != null, "project is null");
        if(!project.matches("^[0-9A-Za-z_]+$")) {
            throw new IllegalArgumentException("Project id is not valid.");
        }
    }
    public static void checkCollection(String collection) {
        checkArgument(collection != null, "collection is null");
        if(!collection.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Collection is not valid.");
        }
    }

    public static String checkTableColumn(String column, String type) {
        checkArgument(type != null, type+" is null");
        if(!column.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException(type+" is not valid.");
        }
        return column;
    }

    public static void checkArgument(boolean expression, @Nullable String errorMessage) {
        if (!expression) {
            if(errorMessage == null) {
                throw new RakamException(HttpResponseStatus.BAD_REQUEST);
            } else {
                throw new RakamException(errorMessage, HttpResponseStatus.BAD_REQUEST);
            }
        }
    }
}
