package org.rakam.util;

import io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.util.Locale.ENGLISH;

public final class ValidationUtil {
    private ValidationUtil()
            throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static String checkProject(String project) {
        checkArgument(project != null, "project is null");
        if (!project.matches("^[0-9A-Za-z_]+$")) {
            throw new IllegalArgumentException("Project id is not valid. It must be alphanumeric and should not include empty space.");
        }
        return project.toLowerCase(ENGLISH);
    }

    public static String checkProject(String project, char character) {
        return character + checkProject(project).replaceAll("\"", "") + character;
    }

    public static <T> T checkNotNull(T value, String name) {
        checkArgument(value != null, name + " is null");
        return value;
    }

    public static String checkCollection(String collection) {
        return checkCollection(collection, '"');
    }

    public static String checkCollection(String collection, char character) {
        checkCollectionValid(collection);
        return character + collection.replaceAll("\"", "") + character;
    }

    public static String checkCollectionValid(String collection) {
        checkArgument(collection != null, "collection is null");
        checkArgument(!collection.isEmpty(), "collection is empty string");
        if (collection.length() > 100) {
            throw new IllegalArgumentException("Collection name must have maximum 250 characters.");
        }
        return collection;
    }

    public static String checkTableColumn(String column, char escape) {
        return checkTableColumn(column, column, escape);
    }

    public static String checkTableColumn(String column) {
        return checkTableColumn(column, column, '"');
    }

    public static String checkLiteral(String value) {
        return value.replaceAll("'", "''");
    }

    public static String checkTableColumn(String column, String type, char escape) {
        if (column == null) {
            throw new IllegalArgumentException(type + " is null");
        }

        return escape + stripName(column, "field name") + escape;
    }

    public static void checkArgument(boolean expression, @Nullable String errorMessage) {
        if (!expression) {
            if (errorMessage == null) {
                throw new RakamException(BAD_REQUEST);
            } else {
                throw new RakamException(errorMessage, BAD_REQUEST);
            }
        }
    }

    public static String stripName(String name, String type) {
        if (name.isEmpty()) {
            throw new RakamException(type + " is empty", HttpResponseStatus.BAD_REQUEST);
        }

        StringBuilder builder = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char charAt = name.charAt(i);
            if (charAt == '"' || (i == 0 && charAt == ' ')) {
                continue;
            }

            if (Character.isUpperCase(charAt)) {
                if (i > 0) {
                    if (Character.isLowerCase(name.charAt(i - 1))) {
                        builder.append("_");
                    }
                }

                builder.append(Character.toLowerCase(charAt));
            } else {
                builder.append(charAt);
            }
        }

        if (builder.length() == 0) {
            throw new RakamException("Invalid " + type + ": " + name, HttpResponseStatus.BAD_REQUEST);
        }

        int lastIdx = builder.length() - 1;
        if (builder.charAt(lastIdx) == ' ') {
            builder.deleteCharAt(lastIdx);
        }

        return builder.toString();
    }

}
