package org.rakam.analysis.stream;

import com.facebook.presto.Session;
import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilerUtils;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.CachedInstanceBinder;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CursorProcessorCompiler;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.avro.generic.GenericRecord;

import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Predicate;

import static com.facebook.presto.bytecode.Access.*;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.google.common.collect.ImmutableList.copyOf;

public class ExpressionCompiler {
    private final BlockEncodingSerde serde;
    private final Metadata metadata ;
    private final Session session;
    private final TypeManager typeManager;
    private final FeaturesConfig featuresConfig;
    private final ExpressionOptimizer expressionOptimizer;

    @Inject
    public ExpressionCompiler() {
        TransactionManager transactionManager = TransactionManager.createTestTransactionManager();
        Metadata metadata = MetadataManager.createTestMetadataManager();

        this.serde = metadata.getBlockEncodingSerde();
        this.metadata = metadata;
        this.featuresConfig = new FeaturesConfig();
        this.typeManager = metadata.getTypeManager();
        this.session = Session.builder(new SessionPropertyManager())
                .setIdentity(new Identity("user", Optional.empty()))
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLocale(Locale.ENGLISH)
                .setQueryId(QueryId.valueOf("row_expression_compiler"))
                .setTransactionId(transactionManager.beginTransaction(IsolationLevel.REPEATABLE_READ, true, true))
                .build();

        this.expressionOptimizer = new ExpressionOptimizer(metadata.getFunctionRegistry(), metadata.getTypeManager(), session);

    }

    public Predicate<GenericRecord> generate(Expression expression, List<Map.Entry<String, Type>> columns) {
        FilterContext filterContext = analyze(expression, columns);

        ImmutableList<Type> types = copyOf(filterContext.sourceTypes.values());
        AvroRecordCursor cursor = new AvroRecordCursor(types, filterContext.projections);
        Filter filter = filterContext.filter;

        ConnectorSession connectorSession = session.toConnectorSession();

        return genericRecord -> {
            cursor.setRecord(genericRecord);
            return filter.filter(connectorSession, cursor);
        };
    }

    private FilterContext analyze(Expression filterExpression, List<Map.Entry<String, Type>> columns) {
//        filterExpression = rewriteQualifiedNamesToSymbolReferences(filterExpression);
        // TODO
        List<Expression> projectionExpressions = new ArrayList<>();
        Map<Symbol, Integer> sourceLayout = new HashMap<>();
        Map<Integer, Type> sourceTypes = new HashMap<>();
        int[] projectionProxies = new int[columns.size()];
        new DefaultTraversalVisitor<Void, Void>() {
            @Override
            protected Void visitSymbolReference(SymbolReference node, Void context) {
                projectionExpressions.add(node);
                int idx = sourceLayout.size();
                sourceLayout.put(new Symbol(node.getName().toString()), idx);

                int index = 0;
                for (int i = 0; i < columns.size(); i++) {
                    Map.Entry<String, Type> entry = columns.get(i);
                    if (entry.getKey().equals(node.getName())) {
                        sourceTypes.put(index++, entry.getValue());
                        projectionProxies[idx] = i;
                        break;
                    }
                }

                return null;
            }
        }.process(filterExpression, null);

        // compiler uses inputs instead of symbols, so rewrite the expressions first
//        SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
//        Expression rewrittenFilter = ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, filterExpression);

//        List<Expression> rewrittenProjections = new ArrayList<>();
//        for (Expression projection : projectionExpressions) {
//            rewrittenProjections.add(ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, projection));
//        }

//        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(
//                session,
//                metadata,
//                sqlParser,
//                sourceTypes,
//                concat(singleton(rewrittenFilter), rewrittenProjections),
//                ImmutableList.of());

        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, serde, featuresConfig);
//        RowExpression filter = SqlToRowExpressionTranslator.translate(
//                rewrittenFilter, FunctionKind.SCALAR, expressionTypes, functionRegistry, typeManager, session, true);
//        filter = expressionOptimizer.optimize(filter);

//        return new FilterContext(sourceTypes, projectionProxies, compileRowExpression(filter));
        return null;
    }

    private Filter compileRowExpression(RowExpression filter) {
        ParameterizedType className = CompilerUtils.makeClassName(Filter.class.getSimpleName());
        ParameterizedType[] interfaces = {ParameterizedType.type(Filter.class)};
        ParameterizedType type = ParameterizedType.type(Object.class);
        EnumSet<Access> accessList = a(new Access[]{PUBLIC, FINAL});
        ClassDefinition classDefinition = new ClassDefinition(accessList, className, type, interfaces);
        classDefinition.declareDefaultConstructor(a(PUBLIC));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        CursorProcessorCompiler cursorProcessorCompiler = new CursorProcessorCompiler(metadata);
        Method method;
        try {
            method = cursorProcessorCompiler.getClass().getDeclaredMethod("generateFilterMethod", ClassDefinition.class, CallSiteBinder.class, CachedInstanceBinder.class, RowExpression.class);
            method.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }

        try {
            method.invoke(cursorProcessorCompiler, classDefinition, callSiteBinder,
                    new CachedInstanceBinder(classDefinition, callSiteBinder), filter);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }

        Class<? extends Filter> aClass = defineClass(classDefinition, Filter.class, callSiteBinder.getBindings(), getClass().getClassLoader());

        try {
            return aClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Couldn't compile expression", e);
        }
    }


    public interface Filter {
        boolean filter(ConnectorSession session, RecordCursor cursor);
    }

    private static class FilterContext {
        public final Map<Integer, Type> sourceTypes;
        public final int[] projections;
        public final Filter filter;

        private FilterContext(Map<Integer, Type> sourceTypes, int[] projections, Filter filter) {
            this.sourceTypes = sourceTypes;
            this.projections = projections;
            this.filter = filter;
        }
    }
}
