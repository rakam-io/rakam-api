package org.rakam.util;

import javax.annotation.Nullable;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;


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
            throw new RakamException("Collection is not valid.", BAD_REQUEST);
        }
    }

    public static String checkTableColumn(String column, String type) {
        checkArgument(type != null, type+" is null");
        if(!column.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new RakamException(type+" is not valid.", BAD_REQUEST);
        }
        return column;
    }

    public static void checkArgument(boolean expression, @Nullable String errorMessage) {
        if (!expression) {
            if(errorMessage == null) {
                throw new RakamException(BAD_REQUEST);
            } else {
                throw new RakamException(errorMessage, BAD_REQUEST);
            }
        }
    }
}
