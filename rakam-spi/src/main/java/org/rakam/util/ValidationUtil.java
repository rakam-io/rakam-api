package org.rakam.util;

import org.rakam.collection.SchemaField;

import javax.annotation.Nullable;

import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public final class ValidationUtil
{
    private ValidationUtil()
            throws InstantiationException
    {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static void checkProject(String project)
    {
        checkArgument(project != null, "project is null");
        if (!project.matches("^[0-9A-Za-z_]+$")) {
            throw new IllegalArgumentException("Project id is not valid.");
        }
    }

    public static <T> T checkNotNull(T value, String name)
    {
        checkArgument(value != null, name+" is null");
        return value;
    }

    public static String checkCollection(String collection)
    {
        return checkCollection(collection, '"');
    }

    public static String checkCollection(String collection, char character)
    {
        checkCollectionValid(collection);
        return character + collection.replaceAll("\"", "").toLowerCase(Locale.ENGLISH) + character;
    }

    public static void checkCollectionValid(String collection)
    {
        checkArgument(collection != null, "collection is null");
        checkArgument(collection.isEmpty(), "collection is empty string");
        if (collection.length() > 250) {
            throw new IllegalArgumentException("Collection name must have maximum 250 characters.");
        }
    }

    public static String checkTableColumn(String column, char escape)
    {
        return checkTableColumn(column, column, escape);
    }

    public static String checkTableColumn(String column)
    {
        return checkTableColumn(column, column, '"');
    }

    public static String checkLiteral(String value)
    {
        return value.replaceAll("'", "''");
    }

    public static String checkTableColumn(String column, String type, char escape)
    {
        if (column == null) {
            throw new IllegalArgumentException(type + " is null");
        }

        return escape + SchemaField.stripName(column) + escape;
    }

    public static void checkArgument(boolean expression, @Nullable String errorMessage)
    {
        if (!expression) {
            if (errorMessage == null) {
                throw new RakamException(BAD_REQUEST);
            }
            else {
                throw new RakamException(errorMessage, BAD_REQUEST);
            }
        }
    }

}
