package org.rakam.clickhouse.analysis;

import com.facebook.presto.sql.RakamExpressionFormatter;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.util.RakamException;

import java.util.Optional;
import java.util.function.Function;

public class ClickhouseExpressionFormatter
        extends RakamExpressionFormatter {
    public ClickhouseExpressionFormatter(Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, char escape) {
        super(tableNameMapper, columnNameMapper, escape);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, char escapeIdentifier) {
        return new ClickhouseExpressionFormatter(tableNameMapper, Optional.of(columnNameMapper), escapeIdentifier).process(expression, null);
    }

    private static void throwException() {
        throw new RakamException("Clickhouse doesn't support NULL, please use empty string for String type, 0 for numeric and timestamp / date values, zero-size items array and map types.", HttpResponseStatus.BAD_REQUEST);
    }

    @Override
    protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
//        return "(" + process(node.getValue(), unmangleNames) + " = '')";
        throwException();
        return null;
    }

    @Override
    protected String visitNullLiteral(NullLiteral node, Void context) {
        throw new RakamException("Clickhouse doesn't support NULL", HttpResponseStatus.BAD_REQUEST);
    }

    @Override
    protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
//        return "(" + process(node.getValue(), unmangleNames) + " != '')";
        throwException();
        return null;
    }
}
