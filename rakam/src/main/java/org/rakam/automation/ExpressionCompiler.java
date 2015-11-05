package org.rakam.automation;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import net.openhft.compiler.CompilerUtils;
import org.rakam.collection.Event;

import java.util.function.Predicate;

import static org.rakam.util.ValidationUtil.checkTableColumn;

public class ExpressionCompiler {
    public static Predicate<Event> compile(String expressionStr) {
        final Expression expression = new SqlParser().createExpression(expressionStr);
        final String javaExp = new JavaSourceAstVisitor().process(expression, false);
        String className = "org.rakam.automation.compiled.Predicate1";
        String javaCode = String.format("package org.rakam.automation.compiled;\n" +
                "import org.rakam.collection.Event;\n" +
                "import org.apache.avro.generic.GenericRecord;\n" +
                "import java.lang.Comparable;\n" +
                "import java.util.function.Predicate;\n" +
                "public class Predicate1 implements Predicate<Event> {\n" +
                "    public boolean test(Event event) {\n" +
                "        GenericRecord props = event.properties();\n" +
                "        return %s;\n" +
                "    }\n" +
                "}\n", javaExp);

        try {
            Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
            return (Predicate) aClass.newInstance();
        } catch (ClassNotFoundException|InstantiationException|IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class JavaSourceAstVisitor extends AstVisitor<String, Boolean> {
        public String visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean context) {
            return formatBinaryExpression(getLogicalContext(node.getType()), node.getLeft(), node.getRight(), context);
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Boolean context) {
            return String.format(getComparisonFormat(node.getType()), process(node.getRight(), context), process(node.getLeft(), context));
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right, boolean unmangleNames)
        {
            return '(' + process(left, unmangleNames) + ' ' + operator + ' ' + process(right, unmangleNames) + ')';
        }

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Boolean unmangleNames) {
            if (node.getName().getPrefix().isPresent()) {
                throw new IllegalArgumentException("field reference is invalid");
            }
            final String suffix = node.getName().getSuffix();
            checkTableColumn(suffix, "field reference is invalid");
            return "props.get(\""+suffix+"\")";
        }

        @Override
        protected String visitLiteral(Literal node, Boolean context) {
            return node.toString();
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

        private String getComparisonFormat(ComparisonExpression.Type type) {
            switch (type) {
                case EQUAL:
                    return "%2$s instanceof Comparable && ((Comparable) %2$s).equals(%1$s)";
                case NOT_EQUAL:
                    return "%2$s instanceof Comparable && !((Comparable) %2$s).equals(%1$s)";
                case LESS_THAN:
                    return "%2$s instanceof Comparable && ((Comparable) %2$s).compareTo(%1$s) > 0";
                case GREATER_THAN:
                    return "%2$s instanceof Comparable && ((Comparable) %2$s).compareTo(%1$s) < 0";
                case GREATER_THAN_OR_EQUAL:
                    return "%2$s instanceof Comparable && ((Comparable) %2$s).compareTo(%1$s) <= 0";
                case LESS_THAN_OR_EQUAL:
                    return "%2$s instanceof Comparable && ((Comparable) %2$s).compareTo(%1$s) >= 0";
                default:
                    throw new IllegalStateException();
            }
        }


        @Override
        protected String visitNode(Node node, Boolean context) {
            throw new UnsupportedOperationException();
        }
    }
}
