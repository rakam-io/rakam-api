package org.rakam.presto.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecutor;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class PrestoMaterializedViewService extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";
    public final static SqlParser sqlParser = new SqlParser();

    @Inject
    public PrestoMaterializedViewService(QueryExecutor executor, QueryMetadataStore database, Clock clock) {
        super(executor, database, clock);
    }

    @Override
    public CompletableFuture<Void> create(MaterializedView materializedView) {
        sqlParser.createStatement(materializedView.query).accept(new DefaultTraversalVisitor<Void, Void>() {
            @Override
            protected Void visitSelect(Select node, Void context) {
                for (SelectItem selectItem : node.getSelectItems()) {
                    if(selectItem instanceof AllColumns) {
                        throw new RakamException("Wildcard in select items is not supported in materialized views.", BAD_REQUEST);
                    }
                    if(selectItem instanceof SingleColumn) {
                        SingleColumn selectColumn = (SingleColumn) selectItem;
                        if(!selectColumn.getAlias().isPresent() && !(selectColumn.getExpression() instanceof QualifiedNameReference)) {
                            throw new RakamException(String.format("Column '%s' must have alias", selectColumn.getExpression().toString()), BAD_REQUEST);
                        } else {
                            continue;
                        }
                    }

                    throw new IllegalStateException();
                }
                return null;
            }
        }, null);
        return super.create(materializedView);
    }
}
