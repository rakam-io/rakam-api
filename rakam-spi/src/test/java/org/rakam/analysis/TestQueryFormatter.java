package org.rakam.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.RakamSqlFormatter.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import org.rakam.util.QueryFormatter;
import org.testng.annotations.Test;

import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class TestQueryFormatter {
    @Test
    public void testSimpleExpression() throws Exception {
        Expression expression = new SqlParser().createExpression("test = 'test'");

        assertEquals("(\"dummy\".\"test\" = 'test')", RakamSqlFormatter.formatExpression(expression,
                name -> {
                    throw new UnsupportedOperationException();
                },
                name -> "\"dummy\"." + name.getParts().stream().map(ExpressionFormatter::formatIdentifier)
                        .collect(Collectors.joining("."))));
    }

    @Test
    public void testSimpleQuery() throws Exception {
        Statement statement = new SqlParser().createStatement("select * from testcollection");

        assertEquals("SELECT *\n" +
                "   FROM\n" +
                "     dummy", QueryFormatter.format(statement, name -> "dummy").trim());
    }

    @Test
    public void testJoinQuery() throws Exception {
        Statement statement = new SqlParser().createStatement
                ("select * from testcollection join anothercollection on (anothercollection.test = testcollection.test)");

        // TODO: decide if we should also format expressions in QueryFormatter
        assertEquals("SELECT *\n" +
                "   FROM\n" +
                "     (dummy\n" +
                "   INNER JOIN dummy ON ((\"anothercollection\".\"test\" = \"testcollection\".\"test\")))",
                QueryFormatter.format(statement, name -> "dummy").trim());
    }

    @Test
    public void testQueryWithCTE() throws Exception {
        Statement statement = new SqlParser().createStatement("with test as (select * from collection) select * from test");

        assertEquals("WITH\n" +
                "     test AS (\n" +
                "      SELECT *\n" +
                "      FROM\n" +
                "        dummy\n" +
                "   ) \n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     test", QueryFormatter.format(statement, name -> "dummy").trim());
    }

    @Test
    public void testQueryWithCTEDuplicateName() throws Exception {
        Statement statement = new SqlParser().createStatement("with test as (select * from collection) select * from collection.test");

        assertEquals("WITH\n" +
                "     test AS (\n" +
                "      SELECT *\n" +
                "      FROM\n" +
                "        dummy\n" +
                "   ) \n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     dummy", QueryFormatter.format(statement, name -> "dummy").trim());
    }

    @Test
    public void testExpressionFormatterFormatTable() throws Exception {
        Expression expression = new SqlParser().createExpression("test in (select id from testcollection)");

        assertEquals("(\"test\" IN (SELECT \"id\"\n" +
                "FROM\n" +
                "  \"schema\".\"testcollection\"\n" +
                "))", RakamSqlFormatter.formatExpression(expression,
                name -> "\"schema\"." + name.getParts().stream().map(ExpressionFormatter::formatIdentifier).collect(Collectors.joining(".")),
                name -> name.getParts().stream().map(ExpressionFormatter::formatIdentifier)
                        .collect(Collectors.joining("."))));
    }
}
