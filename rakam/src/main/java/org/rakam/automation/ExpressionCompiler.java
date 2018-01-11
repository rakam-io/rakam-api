package org.rakam.automation;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.google.common.base.Throwables;
import net.openhft.compiler.CompilerUtils;
import org.rakam.collection.Event;

import java.util.function.Predicate;

import static org.rakam.util.ValidationUtil.checkTableColumn;

public final class ExpressionCompiler {

    private static final SqlParser sqlParser = new SqlParser();

    private ExpressionCompiler()
            throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static Predicate<Event> compile(String expressionStr)
            throws UnsupportedOperationException {
        final Expression expression;
        synchronized (sqlParser) {
            expression = sqlParser.createExpression(expressionStr);
        }
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
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class JavaSourceAstVisitor
            extends AstVisitor<String, Boolean> {
        public String visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean context) {
            return formatBinaryExpression(getLogicalContext(node.getType()), node.getLeft(), node.getRight(), context);
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Boolean context) {
            return String.format(getComparisonFormat(node.getType()), process(node.getRight(), context), process(node.getLeft(), context));
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right, boolean unmangleNames) {
            return '(' + process(left, unmangleNames) + ' ' + operator + ' ' + process(right, unmangleNames) + ')';
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Boolean context) {
            StringBuilder builder = new StringBuilder();
            // TODO: handle this in a proper way.
            if (!(node.getPattern() instanceof StringLiteral)) {
                throw new UnsupportedOperationException();
            }

            String value = ((StringLiteral) node.getPattern()).getValue();

            String process = process(node.getValue(), context);
            builder.append('(')
                    .append(process).append(" instanceof String && ((String) ").append(process).append(").");

            boolean starts = false;
            boolean ends = false;
            int length = value.length();
            for (int i = -1; (i = value.indexOf('%', i + 1)) != -1; ) {
                if (i == 0) {
                    starts = true;
                } else if (i + 1 == length) {
                    ends = true;
                } else {
                    throw new UnsupportedOperationException();
                }
            }

            if (starts && ends) {
                builder.append("contains(\"").append(value.substring(1, length - 1)).append("\")");
            } else if (ends) {
                builder.append("endsWith(\"").append(value.substring(0, length - 1)).append("\")");
            } else if (starts) {
                builder.append("startsWith(\"").append(value.substring(1, length - 2)).append("\")");
            }

            if (node.getEscape() != null) {
                throw new UnsupportedOperationException();
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitIdentifier(Identifier node, Boolean context) {
            return "props.get(\"" + checkTableColumn(node.getValue(), "field reference is invalid", '"') + "\")";
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

        private String getComparisonFormat(ComparisonExpressionType type) {
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
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Boolean context) {
            return process(node.getValue(), context) + " != null";
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Boolean context) {
            return process(node.getValue(), context) + " == null";
        }

        @Override
        protected String visitNode(Node node, Boolean context) {
            throw new UnsupportedOperationException();
        }
    }
}
