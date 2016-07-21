package org.rakam.clickhouse.analysis;

import com.facebook.presto.sql.RakamExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.util.RakamException;

import java.util.Optional;
import java.util.function.Function;

public class ClickhouseExpressionFormatter
        extends RakamExpressionFormatter
{
    public ClickhouseExpressionFormatter(Function<QualifiedName, String> tableNameMapper, Optional<Function<QualifiedName, String>> columnNameMapper, char escape)
    {
        super(tableNameMapper, columnNameMapper, escape);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Function<QualifiedName, String> columnNameMapper, char escapeIdentifier)
    {
        return new ClickhouseExpressionFormatter(tableNameMapper, Optional.of(columnNameMapper), escapeIdentifier).process(expression, false);
    }

    @Override
    protected String visitIsNullPredicate(IsNullPredicate node, Boolean unmangleNames)
    {
//        return "(" + process(node.getValue(), unmangleNames) + " = '')";
        throwException();
        return null;
    }

    @Override
    protected String visitNullLiteral(NullLiteral node, Boolean unmangleNames)
    {
        throw new RakamException("Clickhouse doesn't support NULL", HttpResponseStatus.BAD_REQUEST);
    }

    @Override
    protected String visitIsNotNullPredicate(IsNotNullPredicate node, Boolean unmangleNames)
    {
//        return "(" + process(node.getValue(), unmangleNames) + " != '')";
        throwException();
        return null;
    }

    private static void throwException()
    {
        throw new RakamException("Clickhouse doesn't support NULL, please use empty string for String type, 0 for numeric and timestamp / date values, zero-size items array and map types.", HttpResponseStatus.BAD_REQUEST);
    }
}
