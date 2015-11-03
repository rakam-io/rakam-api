import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import net.openhft.compiler.CompilerUtils;
import org.junit.Test;
import org.rakam.collection.Event;

import java.util.function.Predicate;

public class ExpressionCompiler {

    @Test
    public void testName() throws Exception {
        final Expression expression = new SqlParser().createExpression("a = 5 and b > 2");
        final StringBuilder builder = new StringBuilder();
        new ExpressionFormatter.Formatter() {
            public String visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean context) {
                return '(' + this.process(node.getLeft(), context) + ' ' + getLogicalContext(node.getType()) + ' ' + this.process(node.getRight(), context) + ')';
            }

            @Override
            protected String visitQualifiedNameReference(QualifiedNameReference node, Boolean unmangleNames) {
                return super.visitQualifiedNameReference(node, unmangleNames);
            }

            private String getLogicalContext(LogicalBinaryExpression.Type type) {
                switch (type) {
                    case AND:
                        return "&&";
                    case OR:
                        return "||";
                    default:
                        throw new IllegalStateException();
                }
            }
        }.process(expression, false);
        String className = "org.rakam.automation.compiled.Predicate1";
        String javaCode = "package org.rakam.automation.compiled;\n" +
                "import org.rakam.collection.Event;\n" +
                "import java.util.function.Predicate;\n" +
                "public class Predicate1 implements Predicate<Event> {\n" +
                "    public boolean test(Event event) {\n" +
                "        return true;\n" +
                "    }\n" +
                "}\n";
        Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
        Predicate<Event> runner = (Predicate) aClass.newInstance();
        final boolean test = runner.test(null);
        System.out.println(test);

    }
}
