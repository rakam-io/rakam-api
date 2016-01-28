package org.rakam.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.RakamSqlFormatter.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Joiner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestQueryFormatter {

    @Test
    public void testExpressionFormatter() throws Exception {
        Expression expression = new SqlParser().createExpression("test = 'test'");

        RakamSqlFormatter.formatExpression(expression, new Function<QualifiedName, String>() {
            @Override
            public String apply(QualifiedName name) {
                return null;
            }
        }, new Function<QualifiedName, String>() {
            @Override
            public String apply(QualifiedName name) {
                List<String> parts = name.getParts().stream()
                        .map(ExpressionFormatter::formatIdentifier).collect(Collectors.toList());
                return Joiner.on('.').join(parts);
            }
        });

    }
}
