package org.rakam.util;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/09/14 01:04.
 */
public class ValidationUtil {
    public static void checkProject(String project) {
        checkNotNull(project, "project is null");
        if(!project.matches("^[0-9A-Za-z]+$")) {
            throw new IllegalArgumentException("Project id is not valid.");
        }
    }
    public static void checkCollection(String collection) {
        checkNotNull(collection, "collection is null");
        if(!collection.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Collection is not valid.");
        }
    }

    public static void checkTableColumn(String column, String type) {
        checkNotNull(type, type+" is null");
        if(!column.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException(type+" is not valid.");
        }
    }
}
