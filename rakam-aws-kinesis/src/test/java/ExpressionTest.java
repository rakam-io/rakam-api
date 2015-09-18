import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.byteCode.Access;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.sql.gen.CursorProcessorCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.singleton;
import static java.util.Locale.ENGLISH;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 19:07.
 */
public class ExpressionTest {
    private final static Session session = Session.builder(new SessionPropertyManager())
            .setUser("user")
            .setSource("test")
            .setCatalog("stream")
            .setSchema("default")
            .setTimeZoneKey(TimeZoneKey.UTC_KEY)
            .setLocale(ENGLISH)
            .build();
    private final MetadataManager metadata = createTestMetadataManager();

    public static MetadataManager createTestMetadataManager()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        TypeManager typeManager = new TypeRegistry();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        SplitManager splitManager = new SplitManager();
        BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(typeManager);
        return new MetadataManager(featuresConfig,
                typeManager, splitManager,
                blockEncodingSerde, sessionPropertyManager);
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

        TypeManager typeManager = new TypeRegistry();
        BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(typeManager);

        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, blockEncodingSerde, false);
        RowExpression filter = SqlToRowExpressionTranslator.translate(
                rewrittenFilter, expressionTypes, functionRegistry, typeManager, session, true);

        ClassDefinition classDefinition = new ClassDefinition(Access.a(new Access[]{Access.PUBLIC, Access.FINAL}), CompilerUtils.makeClassName(Filter.class.getSimpleName()), ParameterizedType.type(Object.class), new ParameterizedType[]{ParameterizedType.type(Filter.class)});
        classDefinition.declareDefaultConstructor(a(PUBLIC));
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        CursorProcessorCompiler cursorProcessorCompiler = new CursorProcessorCompiler(metadata);
        Method method;
        try {
            method = cursorProcessorCompiler.getClass().getDeclaredMethod("generateFilterMethod", ClassDefinition.class, CallSiteBinder.class, RowExpression.class);
            method.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }

        try {
            method.invoke(cursorProcessorCompiler, classDefinition, callSiteBinder, filter);
        } catch (IllegalAccessException|InvocationTargetException e) {
            throw Throwables.propagate(e);
        }

        Class<? extends Filter> aClass = defineClass(classDefinition, Filter.class, callSiteBinder.getBindings(), getClass().getClassLoader());

        try {
            Filter predicate = aClass.newInstance();
            boolean filterQ = predicate.filter(session.toConnectorSession(), new MyRecordCursor());
            System.out.println(filterQ);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
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

    public interface Filter {
        boolean filter(ConnectorSession session, RecordCursor cursor);
    }
}
