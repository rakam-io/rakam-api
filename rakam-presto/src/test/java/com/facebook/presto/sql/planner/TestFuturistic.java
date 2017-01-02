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
        QueryPlanner queryPlanner = new QueryPlanner(analysis, new SymbolAllocator(), new PlanNodeIdAllocator(), new MetadataSimulator(), build);
        RelationPlan plan = queryPlanner.plan((Query) statement);
        EffectivePredicateExtractor.extract(plan.getRoot(), ImmutableMap.of(new Symbol("ali"), BIGINT));
    }

    private static class MetadataSimulator
            implements Metadata
    {
        @Override
        public void verifyComparableOrderableContract()
        {

        }

        @Override
        public Type getType(TypeSignature typeSignature)
        {
            return null;
        }

        @Override
        public boolean isAggregationFunction(QualifiedName qualifiedName)
        {
            return false;
        }

        @Override
        public List<SqlFunction> listFunctions()
        {
            return null;
        }

        @Override
        public void addFunctions(List<? extends SqlFunction> list)
        {

        }

        @Override
        public boolean schemaExists(Session session, CatalogSchemaName catalogSchemaName)
        {
            return false;
        }

        @Override
        public List<String> listSchemaNames(Session session, String s)
        {
            return null;
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName qualifiedObjectName)
        {
            return null;
        }

        @Override
        public List<TableLayoutResult> getLayouts(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> optional)
        {
            return null;
        }

        @Override
        public TableLayout getLayout(Session session, TableLayoutHandle tableLayoutHandle)
        {
            return null;
        }

        @Override
        public Optional<Object> getInfo(Session session, TableLayoutHandle tableLayoutHandle)
        {
            return null;
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return null;
        }

        @Override
        public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix qualifiedTablePrefix)
        {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            return null;
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return null;
        }

        @Override
        public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix qualifiedTablePrefix)
        {
            return null;
        }

        @Override
        public void createSchema(Session session, CatalogSchemaName catalogSchemaName, Map<String, Object> map)
        {

        }

        @Override
        public void dropSchema(Session session, CatalogSchemaName catalogSchemaName)
        {

        }

        @Override
        public void renameSchema(Session session, CatalogSchemaName catalogSchemaName, String s)
        {

        }

        @Override
        public void createTable(Session session, String s, ConnectorTableMetadata connectorTableMetadata)
        {

        }

        @Override
        public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName qualifiedObjectName)
        {

        }

        @Override
        public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle columnHandle, String s)
        {

        }

        @Override
        public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata columnMetadata)
        {

        }

        @Override
        public void dropTable(Session session, TableHandle tableHandle)
        {

        }

        @Override
        public TableIdentity getTableIdentity(Session session, TableHandle tableHandle)
        {
            return null;
        }

        @Override
        public TableIdentity deserializeTableIdentity(Session session, String s, byte[] bytes)
        {
            return null;
        }

        @Override
        public ColumnIdentity getColumnIdentity(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return null;
        }

        @Override
        public ColumnIdentity deserializeColumnIdentity(Session session, String s, byte[] bytes)
        {
            return null;
        }

        @Override
        public Optional<NewTableLayout> getNewTableLayout(Session session, String s, ConnectorTableMetadata connectorTableMetadata)
        {
            return null;
        }

        @Override
        public OutputTableHandle beginCreateTable(Session session, String s, ConnectorTableMetadata connectorTableMetadata, Optional<NewTableLayout> optional)
        {
            return null;
        }

        @Override
        public void finishCreateTable(Session session, OutputTableHandle outputTableHandle, Collection<Slice> collection)
        {

        }

        @Override
        public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle tableHandle)
        {
            return null;
        }

        @Override
        public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
        {
            return null;
        }

        @Override
        public void finishInsert(Session session, InsertTableHandle insertTableHandle, Collection<Slice> collection)
        {

        }

        @Override
        public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
        {
            return null;
        }

        @Override
        public boolean supportsMetadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
        {
            return false;
        }

        @Override
        public OptionalLong metadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
        {
            return null;
        }

        @Override
        public TableHandle beginDelete(Session session, TableHandle tableHandle)
        {
            return null;
        }

        @Override
        public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> collection)
        {

        }

        @Override
        public Optional<ConnectorId> getCatalogHandle(Session session, String s)
        {
            return null;
        }

        @Override
        public Map<String, ConnectorId> getCatalogNames(Session session)
        {
            return null;
        }

        @Override
        public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix qualifiedTablePrefix)
        {
            return null;
        }

        @Override
        public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix qualifiedTablePrefix)
        {
            return null;
        }

        @Override
        public Optional<ViewDefinition> getView(Session session, QualifiedObjectName qualifiedObjectName)
        {
            return null;
        }

        @Override
        public void createView(Session session, QualifiedObjectName qualifiedObjectName, String s, boolean b)
        {

        }

        @Override
        public void dropView(Session session, QualifiedObjectName qualifiedObjectName)
        {

        }

        @Override
        public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> set, Set<ColumnHandle> set1, TupleDomain<ColumnHandle> tupleDomain)
        {
            return null;
        }

        @Override
        public void grantTablePrivileges(Session session, QualifiedObjectName qualifiedObjectName, Set<Privilege> set, String s, boolean b)
        {

        }

        @Override
        public void revokeTablePrivileges(Session session, QualifiedObjectName qualifiedObjectName, Set<Privilege> set, String s, boolean b)
        {

        }

        @Override
        public FunctionRegistry getFunctionRegistry()
        {
            return null;
        }

        @Override
        public ProcedureRegistry getProcedureRegistry()
        {
            return null;
        }

        @Override
        public TypeManager getTypeManager()
        {
            return null;
        }

        @Override
        public BlockEncodingSerde getBlockEncodingSerde()
        {
            return null;
        }

        @Override
        public SessionPropertyManager getSessionPropertyManager()
        {
            return null;
        }

        @Override
        public SchemaPropertyManager getSchemaPropertyManager()
        {
            return null;
        }

        @Override
        public TablePropertyManager getTablePropertyManager()
        {
            return null;
        }
    }
}
