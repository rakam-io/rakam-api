package org.rakam;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.IdentifierSymbol;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Statement;
import org.testng.annotations.Test;

public class TestQueryMap {
    @Test
    public void testName() throws Exception {
        String sql = "select selami:timestamp, melami:varchar from deneme where ali:timestamp is not null and veli is null group by demo";
        SqlParserOptions options = new SqlParserOptions().allowIdentifierSymbol(IdentifierSymbol.COLON);
        Statement statement = new SqlParser(options).createStatement(sql);

        String s = RakamSqlFormatter.formatSql(statement,
                name -> String.format("(SELECT * FROM events WHERE collection_name = '%s')", name.toString()),
                name -> "\"$data\"['" + name + "']", '"');
        System.out.println(s);
    }
}
