package org.rakam.report;

import com.facebook.presto.sql.tree.Expression;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.util.ValidationUtil;

import java.util.Optional;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static java.lang.String.format;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.*;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractRetentionQueryExecutor implements RetentionQueryExecutor {
    private final char escapeIdentifier;

    public AbstractRetentionQueryExecutor(char escapeIdentifier)
    {
        this.escapeIdentifier = escapeIdentifier;
    }

    protected String getTableSubQuery(String collection,
                                      String connectorField,
                                      Optional<Boolean> isText,
                                      String timeColumn,
                                      Optional<String> dimension,
                                      String timePredicate,
                                      Optional<Expression> filter) {
        return format("select %s as date, %s %s from %s where _time %s %s",
                String.format(timeColumn, "_time"),
                dimension.isPresent() ? checkTableColumn(dimension.get(), "dimension", '"') + " as dimension, " : "",
                isText.map(text -> String.format("%s as %s",
                        checkTableColumn(connectorField, escapeIdentifier),
                        checkTableColumn(connectorField, escapeIdentifier))).orElse(connectorField),
                checkCollection(collection),
                timePredicate,
                filter.isPresent() ? "and " + formatExpression(filter.get(), reference -> {
                    throw new UnsupportedOperationException();
                }, escapeIdentifier) : "",
                dimension.map(v -> ", 2").orElse(""));
    }

    protected String getTimeExpression(DateUnit dateUnit) {
        if (dateUnit == DAY) {
            return "cast(%s as date)";
        } else if (dateUnit == WEEK) {
            return "cast(date_trunc('week', %s) as date)";
        } else if (dateUnit == MONTH) {
            return "cast(date_trunc('month', %s) as date)";
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
