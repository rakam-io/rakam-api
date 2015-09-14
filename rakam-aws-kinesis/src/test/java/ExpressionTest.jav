import com.facebook.presto.Session;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ByteCodeExpressionVisitor;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Locale.ENGLISH;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 19:07.
 */
public class ExpressionTest {
    private final static Session session = Session.builder()
            .setUser("user")
            .setSource("test")
            .setCatalog("stream")
            .setSchema("default")
            .setTimeZoneKey(TimeZoneKey.UTC_KEY)
            .setLocale(ENGLISH)
            .build();
    private final MetadataManager metadata = new MetadataManager();

    private Map<Integer, Type> getInputTypes(Map<Symbol, Integer> layout, List<Type> types)
    {
        ImmutableMap.Builder<Integer, Type> inputTypes = ImmutableMap.builder();
        for (Integer input : ImmutableSet.copyOf(layout.values())) {
            Type type = types.get(input);
            inputTypes.put(input, type);
        }
        return inputTypes.build();
    }

    @Test
    public void test() {

        SqlParser sqlParser = new SqlParser();
        Expression filterExpression = sqlParser.createExpression("ali = 4");

        List<Expression> projectionExpressions = new ArrayList<>();
        Map<Symbol, Integer> sourceLayout = new HashMap<>();
        Map<Integer, Type> sourceTypes = new HashMap<>();
        filterExpression.accept(new DefaultTraversalVisitor<Void, Void>() {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context) {
                projectionExpressions.add(node);
                int idx = sourceLayout.size();
                sourceLayout.put(Symbol.fromQualifiedName(node.getName()), idx);
                sourceTypes.put(idx, BigintType.BIGINT);
                return null;
            }
        }, null);



        ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata);

        // compiler uses inputs instead of symbols, so rewrite the expressions first
        SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
        Expression rewrittenFilter = ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, filterExpression);

        List<Expression> rewrittenProjections = new ArrayList<>();
        for (Expression projection : projectionExpressions) {
            rewrittenProjections.add(ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, projection));
        }

        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(
                session,
                metadata,
                sqlParser,
                sourceTypes,
                concat(singleton(rewrittenFilter), rewrittenProjections));

        RowExpression traslatedFilter = SqlToRowExpressionTranslator.translate(rewrittenFilter, expressionTypes, metadata, session, true);
        List<RowExpression> translatedProjections = SqlToRowExpressionTranslator.translate(rewrittenProjections, expressionTypes, metadata, session, true);

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC, FINAL),
                makeClassName(Filter.class.getSimpleName()),
                type(Object.class),
                type(Filter.class));

        classDefinition.declareDefaultConstructor(a(PUBLIC));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(
                context,
                a(PUBLIC),
                "filter",
                type(boolean.class),
                arg("cursor", RecordCursor.class));

        method.comment("Filter: %s", traslatedFilter);

        Variable wasNullVariable = context.declareVariable(type(boolean.class), "wasNull");
        Variable cursorVariable = context.getVariable("cursor");

        RowExpressionVisitor<CompilerContext, ByteCodeNode> fieldReferenceCompiler = fieldReferenceCompiler(cursorVariable, wasNullVariable);
        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, fieldReferenceCompiler, metadata.getFunctionRegistry());

        LabelNode end = new LabelNode("end");
        method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .comment("evaluate filter: " + filterExpression)
                .append(traslatedFilter.accept(visitor, context))
                .comment("if (wasNull) return false;")
                .getVariable(wasNullVariable)
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();


        Class<? extends Filter> aClass = defineClass(classDefinition, Filter.class, callSiteBinder.getBindings(), getClass().getClassLoader());

        try {
            Filter predicate = aClass.newInstance();
            boolean filter = predicate.filter(new MyRecordCursor());
            System.out.println(filter);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        CursorProcessor cursorProcessor = expressionCompiler.compileCursorProcessor(traslatedFilter, translatedProjections, 0);

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BigintType.BIGINT));
        int process = cursorProcessor.process(session.toConnectorSession(), new MyRecordCursor(), 1, pageBuilder);
        System.out.println(process);
    }

    private RowExpressionVisitor<CompilerContext, ByteCodeNode> fieldReferenceCompiler(final Variable cursorVariable, final Variable wasNullVariable)
    {
        return new RowExpressionVisitor<CompilerContext, ByteCodeNode>()
        {
            @Override
            public ByteCodeNode visitInputReference(InputReferenceExpression node, CompilerContext context)
            {
                int field = node.getField();
                Type type = node.getType();

                Class<?> javaType = type.getJavaType();

                Block isNullCheck = new Block(context)
                        .setDescription(format("cursor.get%s(%d)", type, field))
                        .getVariable(cursorVariable)
                        .push(field)
                        .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);

                Block isNull = new Block(context)
                        .putVariable(wasNullVariable, true)
                        .pushJavaDefault(javaType);

                Block isNotNull = new Block(context)
                        .getVariable(cursorVariable)
                        .push(field);

                String methodName = "get" + Primitives.wrap(javaType).getSimpleName();
                isNotNull.invokeInterface(RecordCursor.class, methodName, javaType, int.class);

                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }

            @Override
            public ByteCodeNode visitCall(CallExpression call, CompilerContext context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public ByteCodeNode visitConstant(ConstantExpression literal, CompilerContext context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }
        };
    }

    private static class MyRecordCursor implements RecordCursor, SymbolResolver {
        @Override
        public long getTotalBytes() {
            return 0;
        }

        @Override
        public long getCompletedBytes() {
            return 0;
        }

        @Override
        public long getReadTimeNanos() {
            return 0;
        }

        @Override
        public Type getType(int field) {
            return null;
        }

        @Override
        public boolean advanceNextPosition() {
            return true;
        }

        @Override
        public boolean getBoolean(int field) {
            return false;
        }

        @Override
        public long getLong(int field) {
            return 4;
        }

        @Override
        public double getDouble(int field) {
            return 0;
        }

        @Override
        public Slice getSlice(int field) {
            return null;
        }

        @Override
        public Object getObject(int i) {
            return null;
        }

        @Override
        public boolean isNull(int field) {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public Object getValue(Symbol symbol) {
            return 1;
        }
    }

    public static interface Filter {
        public boolean filter(RecordCursor cursor);
    }
}
