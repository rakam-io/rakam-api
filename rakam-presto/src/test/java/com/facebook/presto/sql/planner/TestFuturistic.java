package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class TestFuturistic {
    @Test(enabled = false)
    public void testName()
            throws Exception {
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
//        EffectivePredicateExtractor.extract(plan.getRoot(), ImmutableMap.of(new Symbol("ali"), BIGINT));
        EffectivePredicateExtractor.extract(plan.getRoot());
    }
}
