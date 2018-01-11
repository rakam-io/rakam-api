package org.rakam.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static com.facebook.presto.sql.RakamSqlFormatter.Formatter.formatQuery;
import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static org.testng.Assert.assertEquals;

public class TestQueryFormatter {
    @Test
    public void testSimpleExpression()
            throws Exception {
        Expression expression = new SqlParser().createExpression("test = 'test'");

        assertEquals(formatExpression(expression,
                name -> {
                    throw new UnsupportedOperationException();
                },
                name -> "\"dummy\".\"" + name + "\"", '"'), "(\"dummy\".\"test\" = 'test')");
    }

    @Test
    public void testSimpleQuery()
            throws Exception {
        Statement statement = new SqlParser().createStatement("select * from testcollection");

        assertEquals(formatQuery(statement, name -> "dummy", '"').trim(), "SELECT *\n" +
                "   FROM\n" +
                "     dummy");
    }

    @Test
    public void testJoinQuery()
            throws Exception {
        Statement statement = new SqlParser().createStatement
                ("select * from testcollection join anothercollection on (anothercollection.test = testcollection.test)");

        // TODO: decide if we should also format expressions in QueryFormatter
        assertEquals(formatQuery(statement, name -> "dummy", '"').trim(), "SELECT *\n" +
                "   FROM\n" +
                "     (dummy\n" +
                "   INNER JOIN dummy ON (\"anothercollection\".\"test\" = \"testcollection\".\"test\"))");
    }

    @Test
    public void testQueryWithCTE()
            throws Exception {
        Statement statement = new SqlParser().createStatement("with test as (select * from collection) select * from test");

        assertEquals(formatQuery(statement, name -> "dummy", '"').trim(), "WITH\n" +
                "     \"test\" AS (\n" +
                "      SELECT *\n" +
                "      FROM\n" +
                "        dummy\n" +
                "   ) \n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     test");
    }

    @Test
    public void testAlias()
            throws Exception {
        Statement statement = new SqlParser().createStatement("select a as b from test");

        assertEquals(formatQuery(statement, name -> "dummy", '"').trim(), "SELECT \"a\" \"b\"\n" +
                "   FROM\n" +
                "     dummy");
    }

    @Test
    public void testQueryWithCTEDuplicateName()
            throws Exception {
        Statement statement = new SqlParser().createStatement("with test as (select * from collection) select * from collection.test");

        assertEquals(formatQuery(statement, name -> "dummy", '"').trim(), "WITH\n" +
                "     \"test\" AS (\n" +
                "      SELECT *\n" +
                "      FROM\n" +
                "        dummy\n" +
                "   ) \n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     dummy");
    }

    @Test
    public void testExpressionFormatterFormatTable()
            throws Exception {
        Expression expression = new SqlParser().createExpression("test in (select id from testcollection)");

        assertEquals(formatExpression(expression,
                name -> "\"schema\"." + name.getParts().stream().map(e -> formatIdentifier(e, '"')).collect(Collectors.joining(".")),
                name -> '"' + name + '"', '"'), "(\"test\" IN (SELECT \"id\"\n" +
                "FROM\n" +
                "  \"schema\".\"testcollection\"\n" +
                "))");
    }
}
