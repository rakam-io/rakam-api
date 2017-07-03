package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NewTableLayout;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.metadata.ProcedureRegistry;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.ResolvedIndex;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnIdentity;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableIdentity;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestFuturistic
{
    @Test(enabled = false)
    public void testName()
            throws Exception
    {
        String s = "select count(*) from test where ali = 5";
        Statement statement = new SqlParser().createStatement(s);
        Analysis analysis = new Analysis(statement, ImmutableList.of(), false);
        Session build = Session.builder(new SessionPropertyManager())
                .setQueryId(QueryId.valueOf("test"))
                .setCatalog("test")
                .setCatalog("test")
                .setCatalog("test")
                .build();
        QueryPlanner queryPlanner = new QueryPlanner(analysis, new SymbolAllocator(), new PlanNodeIdAllocator(), null, null, build);
        RelationPlan plan = queryPlanner.plan((Query) statement);
        EffectivePredicateExtractor.extract(plan.getRoot(), ImmutableMap.of(new Symbol("ali"), BIGINT));
    }
}
