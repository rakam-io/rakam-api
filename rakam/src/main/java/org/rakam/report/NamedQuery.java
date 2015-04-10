package org.rakam.report;

import com.google.common.collect.Lists;
import org.rakam.collection.FieldType;
import org.rakam.util.JsonHelper;
import org.rakam.util.Tuple;
import org.skife.jdbi.org.antlr.runtime.ANTLRStringStream;
import org.skife.jdbi.org.antlr.runtime.Token;
import org.skife.jdbi.rewriter.colon.ColonStatementLexer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.skife.jdbi.rewriter.colon.ColonStatementLexer.DOUBLE_QUOTED_TEXT;
import static org.skife.jdbi.rewriter.colon.ColonStatementLexer.ESCAPED_TEXT;
import static org.skife.jdbi.rewriter.colon.ColonStatementLexer.LITERAL;
import static org.skife.jdbi.rewriter.colon.ColonStatementLexer.NAMED_PARAM;
import static org.skife.jdbi.rewriter.colon.ColonStatementLexer.QUOTED_TEXT;

/**
* Created by buremba <Burak Emre Kabakcı> on 09/04/15 08:00.
*/
public class NamedQuery {
    Map<String, Tuple<FieldType, Object>> params = new LinkedHashMap<>();
    List<String> queryParts = Lists.newArrayList();

    public NamedQuery(String sqlQuery) {
        parse(sqlQuery);
    }

    public void parse(String sqlQuery) {
        ColonStatementLexer lexer = new ColonStatementLexer(new ANTLRStringStream(sqlQuery));
        Token t = lexer.nextToken();
        while (t.getType() != ColonStatementLexer.EOF) {
            switch (t.getType()) {
                case NAMED_PARAM:
                    params.put(t.getText().substring(1, t.getText().length()), null);
                    break;
                case LITERAL:
                case QUOTED_TEXT:
                case DOUBLE_QUOTED_TEXT: {
                    if(params.size() < queryParts.size()) {
                        int lastIndex = queryParts.size() - 1;
                        queryParts.set(lastIndex, queryParts.get(lastIndex) + t.getText());
                    } else {
                        queryParts.add(t.getText());
                    }
                    break;
                } case ESCAPED_TEXT: {
                    if(params.size() < queryParts.size()) {
                        int lastIndex = queryParts.size() - 1;
                        queryParts.set(lastIndex, queryParts.get(lastIndex) + t.getText().substring(1));
                    } else {
                        queryParts.add(t.getText());
                    }
                    break;
                } default:
                    break;
            }
            t = lexer.nextToken();
        }
    }

    public NamedQuery bind(String name, FieldType fieldType, Object value) {
        Tuple tuple = new Tuple(checkNotNull(fieldType, "fieldType is null"), value);
        params.put(checkNotNull(name, "name is null"), tuple);
        return this;
    }

    public String build() {
        StringBuilder builder = new StringBuilder(queryParts.get(0));

        int i = 1;
        for (Map.Entry<String, Tuple<FieldType, Object>> entry : params.entrySet()) {
            Tuple<FieldType, Object> tuple = entry.getValue();
            if(tuple == null || tuple.v2() == null) {
                builder.append("null");
            } else {
                switch (tuple.v1()) {
                    case STRING:
                        builder.append('\'').append(tuple.v2()).append('\'');
                        break;
                    case LONG:
                        builder.append((Number) tuple.v2());
                        break;
                    case DATE:
                        builder.append("date '").append(tuple.v2()).append('\'');
                        break;
                    case TIME:
                        builder.append("CAST ('").append(tuple.v2()).append("' AS TIME)");
                        break;
                    case BOOLEAN:
                        builder.append((Boolean) tuple.v2());
                        break;
                    case ARRAY:
                        builder.append(JsonHelper.encode((List) tuple.v2()));
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
            builder.append(queryParts.size() == i ? "" : queryParts.get(i++));
        }

        return builder.toString();
    }

    public Set<String> parameters() {
        return params.keySet();
    }
}
