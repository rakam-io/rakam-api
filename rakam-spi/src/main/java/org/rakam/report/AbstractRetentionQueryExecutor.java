package org.rakam.report;

import org.rakam.analysis.RetentionQueryExecutor;

import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.DAY;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.MONTH;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.WEEK;

public abstract class AbstractRetentionQueryExecutor
        implements RetentionQueryExecutor
{
    protected String getTimeExpression(DateUnit dateUnit)
    {
        if (dateUnit == DAY) {
            return "cast(%s as date)";
        }
        else if (dateUnit == WEEK) {
            return "cast(date_trunc('week', %s) as date)";
        }
        else if (dateUnit == MONTH) {
            return "cast(date_trunc('month', %s) as date)";
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
