package org.rakam.util;

import com.google.common.base.CharMatcher;

import javax.annotation.Nullable;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;


public final class ValidationUtil {

    private ValidationUtil() throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static void checkProject(String project) {
        checkArgument(project != null, "project is null");
        if (!project.matches("^[0-9A-Za-z_]+$")) {
            throw new IllegalArgumentException("Project id is not valid.");
        }
    }

    public static void checkCollection(String collection) {
        checkArgument(collection != null, "collection is null");
        if (!collection.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new RakamException("Collection is not valid.", BAD_REQUEST);
        }
    }

    private static final CharMatcher UPPERCASE_CHARACTER_MATCHER = CharMatcher.inRange('A', 'Z');
    private static final CharMatcher LOWERCASE_CHARACTER_MATCHER = CharMatcher.inRange('a', 'z');
    private static final CharMatcher DIGIT_MATCHER = CharMatcher.DIGIT;
    private static final CharMatcher UNDERSCORE_MATCHER = CharMatcher.is('_');

    public static String checkTableColumn(String column) {
        return checkTableColumn(column, column);
    }

    public static String checkTableColumn(String column, String type) {
        if (column == null) {
            throw new IllegalArgumentException(type + " is null");
        }

        StringBuilder builder = new StringBuilder(column.length());

        for (int i = 0; i < column.length(); i++) {
            char c = column.charAt(i);
            if (UPPERCASE_CHARACTER_MATCHER.matches(c)) {
                if (i > 0 && builder.charAt(i - 1) != '_' && !UPPERCASE_CHARACTER_MATCHER.matches(column.charAt(i - 1))) {
                    builder.append('_');
                }
                builder.append(Character.toLowerCase(c));
            } else if (LOWERCASE_CHARACTER_MATCHER.matches(c)) {
                builder.append(c);
            } else if (DIGIT_MATCHER.matches(c)) {
                if (i == 0) {
                    throw new RakamException(type + " is invalid. The first character can't be numeric", BAD_REQUEST);
                }
                builder.append(c);
            } else if (UNDERSCORE_MATCHER.matches(c)) {
                builder.append('_');
            } else {
                throw new RakamException(type + " is invalid. It must be in a form of [A-Za-z0-9]", BAD_REQUEST);
            }
        }

        return builder.toString();
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
}
