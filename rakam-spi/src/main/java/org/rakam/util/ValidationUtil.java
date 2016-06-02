package org.rakam.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.rakam.collection.SchemaField;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
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

    public static String checkCollection(String collection)
    {
        checkArgument(collection != null, "collection is null");
        if (collection.length() > 250) {
            throw new IllegalArgumentException("Collection name must have maximum 250 characters.");
        }
        return "\"" + collection.replaceAll("\"", "").toLowerCase(Locale.ENGLISH) + "\"";
    }

    public static String checkTableColumn(String column)
    {
        return checkTableColumn(column, column);
    }

    public static String checkLiteral(String value)
    {
        return value.replaceAll("'", "''");
    }

    public static String checkTableColumn(String column, String type)
    {
        if (column == null) {
            throw new IllegalArgumentException(type + " is null");
        }

        return "\"" + SchemaField.stripName(column) + "\"";
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
