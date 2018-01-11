package org.rakam;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.*;
import org.rakam.util.RakamException;
import org.rakam.util.SqlUtil;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class test {
    public static void main2(String[] args) {
        Function<QualifiedName, String> qualifiedNameStringFunction = qualifiedName -> {
            throw new IllegalArgumentException();
        };

        Function<String, String> columnMappingFunction = qualifiedName -> "ga:" + qualifiedName;

        String query = "SELECT continent, country, pageviews, users FROM collection.google_analytics where _time between date '2016-01-01' and date '2016-01-01' group by continent, country";
        GARequest.ReportRequest reportRequest = new GARequest.ReportRequest();

        Query statement = (Query) SqlUtil.parseSql(query);
        QueryBody queryBody = statement.getQueryBody();
        if (queryBody instanceof QuerySpecification) {
            QuerySpecification node = (QuerySpecification) queryBody;
            node.getGroupBy().ifPresent(groupBy -> {
                for (GroupingElement groupingElement : groupBy.getGroupingElements()) {
                    for (Set<Expression> expressions : groupingElement.enumerateGroupingSets()) {
                        for (Expression expression : expressions) {
                            String dimension = RakamSqlFormatter.formatExpression(expression, qualifiedNameStringFunction, columnMappingFunction, ' ');
                            reportRequest.dimensions.add(new GARequest.ReportRequest.Dimension(dimension));
                        }
                    }
                }
            });

            if (node.getHaving().isPresent()) {
                throw new IllegalArgumentException();
            }


            node.getLimit().ifPresent((limit -> reportRequest.pageSize = Integer.parseInt(limit)));
            node.getOrderBy().ifPresent((orderBy -> {
                for (SortItem sortItem : orderBy.getSortItems()) {
                    String expression = RakamSqlFormatter.formatExpression(sortItem.getSortKey(), qualifiedNameStringFunction, columnMappingFunction, ' ');
                    reportRequest.orderBys.add(new GARequest.ReportRequest.OrderBy(expression, sortItem.getOrdering()));
                }
            }));


            if (node.getSelect().isDistinct()) {
                throw new IllegalArgumentException();
            }

            for (SelectItem selectItem : node.getSelect().getSelectItems()) {
                if (selectItem instanceof AllColumns) {
                    throw new RakamException("* is not supported in SELECT", BAD_REQUEST);
                }

                SingleColumn singleColumn = (SingleColumn) selectItem;

                if (singleColumn.getAlias().isPresent()) {
                    throw new RakamException("Alias in SELECT is not supported", BAD_REQUEST);
                }

                if (singleColumn.getExpression() instanceof FunctionCall) {
                    throw new RakamException("Function call in SELECT is not supported", BAD_REQUEST);
                }

                String select = RakamSqlFormatter.formatExpression(singleColumn.getExpression(), qualifiedNameStringFunction, columnMappingFunction, ' ');
                if (!reportRequest.dimensions.stream().anyMatch(e -> e.name.equals(select))) {
                    reportRequest.metrics.add(new GARequest.ReportRequest.Metric(select));
                }
            }

            node.getWhere().ifPresent(where -> {
                new AstVisitor<Void, Integer>() {
                    @Override
                    protected Void visitBetweenPredicate(BetweenPredicate node, Integer context) {
                        if ((node.getValue() instanceof Identifier)) {
                            Identifier value = (Identifier) node.getValue();
                            switch (value.getValue()) {
                                case "_time":
                                    LocalDate start;
                                    LocalDate end;
                                    if (node.getMin() instanceof GenericLiteral) {
                                        start = LocalDate.parse(((GenericLiteral) node.getMin()).getValue());
                                    } else {
                                        throw new IllegalArgumentException();
                                    }
                                    if (node.getMax() instanceof GenericLiteral) {
                                        end = LocalDate.parse(((GenericLiteral) node.getMax()).getValue());
                                    } else {
                                        throw new IllegalArgumentException();
                                    }
                                    reportRequest.dateRanges.add(new GARequest.ReportRequest.DateRange(start, end));
                                    break;
                            }
                        } else {
                            throw new IllegalArgumentException();
                        }
                        return super.visitBetweenPredicate(node, context);
                    }
                }.process(where);
            });
        }

        System.out.println(new GARequest(reportRequest));
    }

    public static class GARequest {
        public final ReportRequest reportRequest;

        public GARequest(ReportRequest reportRequest) {
            this.reportRequest = reportRequest;
        }

        public static class ReportRequest {
            public String viewId;
            public int pageSize;
            public String samplingLevel = "LARGE";
            public List<DateRange> dateRanges;
            public List<Metric> metrics;
            public List<Dimension> dimensions;
            public List<OrderBy> orderBys;

            public ReportRequest() {
                dateRanges = new ArrayList<>();
                metrics = new ArrayList<>();
                dimensions = new ArrayList<>();
                orderBys = new ArrayList<>();
            }

            public ReportRequest(String viewId, int pageSize, String samplingLevel, List<DateRange> dateRanges, List<Metric> metrics, List<Dimension> dimensions) {
                this.viewId = viewId;
                this.pageSize = pageSize;
                this.samplingLevel = samplingLevel;
                this.dateRanges = dateRanges;
                this.metrics = metrics;
                this.dimensions = dimensions;
            }

            public static class Metric {
                public final String expression;

                public Metric(String expression) {
                    this.expression = expression;
                }


            }

            public static class OrderBy {
                public final String fieldName;
                public final SortItem.Ordering sortOrder;

                public OrderBy(String fieldName, SortItem.Ordering sortOrder) {
                    this.fieldName = fieldName;
                    this.sortOrder = sortOrder;
                }
            }

            public static class Dimension {
                public final String name;

                public Dimension(String name) {
                    this.name = name;
                }
            }

            public static class DateRange {
                public final LocalDate startDate;
                public final LocalDate endDate;

                public DateRange(LocalDate startDate, LocalDate endDate) {
                    this.startDate = startDate;
                    this.endDate = endDate;
                }
            }
        }
    }
}
