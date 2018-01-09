/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.presto.analysis;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.primitives.Ints;
import org.rakam.analysis.CalculatedUserSet;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.report.*;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.*;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.presto.analysis.PrestoUserService.ANONYMOUS_ID_MAPPING;
import static org.rakam.util.DateTimeUtils.LOCAL_DATE_FORMATTER;
import static org.rakam.util.ValidationUtil.*;

public class PrestoRetentionQueryExecutor
        extends AbstractRetentionQueryExecutor {
    private final Metastore metastore;
    private final MaterializedViewService materializedViewService;
    private final QueryExecutorService executor;
    private final boolean userMappingEnabled;
    private final ProjectConfig projectConfig;

    @Inject
    public PrestoRetentionQueryExecutor(
            ProjectConfig projectConfig,
            QueryExecutorService executor,
            Metastore metastore,
            MaterializedViewService materializedViewService,
            UserPluginConfig userPluginConfig) {
        this.projectConfig = projectConfig;
        this.executor = executor;
        this.metastore = metastore;
        this.materializedViewService = materializedViewService;
        this.userMappingEnabled = userPluginConfig.getEnableUserMapping();
    }

    @Override
    public QueryExecution query(
            RequestContext context,
            Optional<RetentionAction> firstAction,
            Optional<RetentionAction> returningAction,
            DateUnit dateUnit,
            Optional<String> dimension,
            Optional<Integer> period,
            LocalDate startDate, LocalDate endDate,
            ZoneId timezone,
            boolean approximateVal) {
        boolean approximate = true;
        period.ifPresent(e -> checkArgument(e >= 0, "Period must be 0 or a positive value"));

        String timeColumn = getTimeExpression(dateUnit);

        LocalDate start;
        LocalDate end;
        if (dateUnit == MONTH) {
            start = startDate.withDayOfMonth(1);
            end = endDate.withDayOfMonth(1).plus(1, ChronoUnit.MONTHS);
        } else if (dateUnit == WEEK) {
            TemporalField fieldUS = WeekFields.of(Locale.US).dayOfWeek();
            start = startDate.with(fieldUS, 1);
            end = endDate.with(fieldUS, 1).plus(1, ChronoUnit.MONTHS);
        } else if (dateUnit == DAY) {
            start = startDate;
            end = endDate;
        } else {
            throw new IllegalStateException();
        }
        Optional<Integer> range = period.map(v -> Math.min(v, Ints.checkedCast(DAYS.between(start, end))));

        if (range.isPresent() && range.get() < 0) {
            throw new IllegalArgumentException("startDate must be before endDate.");
        }

        if (range.isPresent() && range.get() == 0) {
            return QueryExecution.completedQueryExecution(null, QueryResult.empty());
        }

        Set<CalculatedUserSet> missingPreComputedTables = new HashSet<>();

        String firstActionQuery = generateQuery(context.project, firstAction, projectConfig.getUserColumn(), timeColumn, dimension,
                startDate, endDate, missingPreComputedTables, timezone, approximate);
        String returningActionQuery = generateQuery(context.project, returningAction, projectConfig.getUserColumn(), timeColumn, dimension,
                startDate, endDate, missingPreComputedTables, timezone, approximate);

        if (firstActionQuery == null || returningActionQuery == null) {
            return QueryExecution.completedQueryExecution("", QueryResult.empty());
        }

        String timeSubtraction = diffTimestamps(dateUnit, "data.date", "returning_action.date");

        String dimensionColumn = dimension.isPresent() ? "data.dimension" : "data.date";

        String mergeSetAggregation = dimension.map(v -> approximate ? "merge" : "merge_sets").orElse("");
        String query = format("with first_action as (\n" +
                        "  %s\n" +
                        "), \n" +
                        "returning_action as (\n" +
                        "  %s\n" +
                        ") \n" +
                        "select %s, cast(null as bigint) as lead, cardinality(%s(%s_set)) count from first_action data %s union all\n" +
                        "SELECT * FROM (select %s, %s - 1, greatest(0, cardinality_intersection(%s(data.%s_set), %s(returning_action.%s_set))) count \n" +
                        "from first_action data join returning_action on (data.date < returning_action.date %s) \n" +
                        "%s) ORDER BY 1, 2 NULLS FIRST",
                firstActionQuery, returningActionQuery, dimensionColumn, mergeSetAggregation,
                projectConfig.getUserColumn(), dimension.map(v -> "GROUP BY 1").orElse(""), dimensionColumn, timeSubtraction, mergeSetAggregation, projectConfig.getUserColumn(),
                mergeSetAggregation, projectConfig.getUserColumn(),
                range.map(v -> String.format("AND data.date + interval '%d' day >= returning_action.date", v)).orElse(""),
                dimension.map(v -> "GROUP BY 1, 2").orElse(""));

        return new DelegateQueryExecution(executor.executeQuery(context, query, timezone),
                result -> {
                    result.setProperty("calculatedUserSets", missingPreComputedTables);
                    if (!result.isFailed()) {
                        List<List<Object>> results = result.getResult().stream()
                                .filter(rows -> rows.get(2) != null && !rows.get(2).equals(0L))
                                .collect(Collectors.toList());
                        return new QueryResult(result.getMetadata(), results, result.getProperties());
                    }
                    return result;
                });
    }

    protected String generateQuery(String project,
                                   Optional<RetentionAction> retentionAction,
                                   String connectorField,
                                   String timeColumn,
                                   Optional<String> dimension,
                                   LocalDate startDate,
                                   LocalDate endDate,
                                   Set<CalculatedUserSet> missingPreComputedTables,
                                   ZoneId timezone,
                                   boolean approximate) {

        String timePredicate = String.format("between date '%s' and date '%s' + interval '1' day",
                LOCAL_DATE_FORMATTER.format(startDate),
                LOCAL_DATE_FORMATTER.format(endDate));

        if (!retentionAction.isPresent()) {
            Optional<String> preComputedTable = getPreComputedTable(project, timePredicate, timeColumn, Optional.empty(),
                    dimension, Optional.empty(), missingPreComputedTables, dimension.isPresent(), timezone);

            if (preComputedTable.isPresent()) {
                return preComputedTable.get();
            }

            Map<String, List<SchemaField>> collections = metastore.getCollections(project);
            if (!collections.entrySet().stream().anyMatch(e -> e.getValue().stream().anyMatch(s -> s.getName().equals("_user")))) {
                return null;
            }

            boolean isText = collections.entrySet().stream()
                    .anyMatch(e -> e.getValue().stream().anyMatch(z -> z.getType().equals(STRING)));

            return String.format("select date, %s %s(%s) as %s_set from (%s) group by 1 %s",
                    dimension.map(v -> "dimension, ").orElse(""),
                    approximate ? "approx_set" : "set",
                    connectorField, connectorField,
                    collections.entrySet().stream()
                            .filter(entry -> entry.getValue().stream().anyMatch(e -> e.getName().equals(projectConfig.getUserColumn())))
                            .map(collection -> getTableSubQuery(collection.getValue().stream().anyMatch(e -> e.getName().equals("_device_id")), collection.getKey(), connectorField,
                                    Optional.of(isText),
                                    timeColumn, dimension, startDate, endDate, Optional.empty()))
                            .collect(Collectors.joining(" union all ")), dimension.isPresent() ? ", 2" : "");
        } else {
            String collection = retentionAction.get().collection();

            Optional<String> preComputedTable = getPreComputedTable(project, timePredicate, timeColumn, Optional.of(collection), dimension,
                    retentionAction.get().filter(), missingPreComputedTables, dimension.isPresent(), timezone);

            if (preComputedTable.isPresent()) {
                return preComputedTable.get();
            }

            return String.format("select date, %s %s(%s) as %s_set from (%s) group by 1 %s",
                    dimension.map(v -> "dimension, ").orElse(""),
                    approximate ? "approx_set" : "set",
                    connectorField, connectorField,
                    getTableSubQuery(
                            metastore.getCollection(project, collection).stream().anyMatch(e -> e.getName().equals("_device_id")),
                            collection, connectorField, Optional.empty(),
                            timeColumn, dimension, startDate, endDate, retentionAction.get().filter()), dimension.isPresent() ? ", 2" : "");
        }
    }

    private Optional<String> getPreComputedTable(
            String project, String timePredicate,
            String timeColumn, Optional<String> collection, Optional<String> dimension,
            Optional<Expression> filter, Set<CalculatedUserSet> missingPreComputedTables,
            boolean dimensionRequired, ZoneId zoneId) {
        String tableName = "_users_daily" + collection.map(value -> "_" + value).orElse("") + dimension.map(value -> "_by_" + value).orElse("");

        if (filter.isPresent()) {
            try {
                String preComputedTablePrefix = tableName + "_by_";
                return Optional.of(new PreComputedTableSubQueryVisitor(columnName -> {
//                    if (continuousQueryService.list(project).stream().anyMatch(e ->
//                            e.tableName.equals(preComputedTablePrefix + columnName) && Objects.equals(zoneId.getId(), e.getOptions().get("timezone")))) {
//                        return Optional.of("continuous." + preComputedTablePrefix + columnName);
//                    }
//                    else if (materializedViewService.list(project).stream().anyMatch(
//                            e -> e.tableName.equals(preComputedTablePrefix + columnName) && Objects.equals(zoneId.getId(), e.options.get("timezone")))) {
//                        return Optional.of("materialized." + preComputedTablePrefix + columnName);
//                    }

                    missingPreComputedTables.add(new CalculatedUserSet(collection, Optional.of(columnName)));
                    return Optional.empty();
                }).process(filter.get(), false));
            } catch (UnsupportedOperationException e) {
                return Optional.empty();
            }
        }

//        if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
//            return Optional.of(generatePreCalculatedTableSql(Optional.of(tableName), "continuous", timePredicate, timeColumn, dimensionRequired));
//        }
        else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
            return Optional.of(generatePreCalculatedTableSql(Optional.of(tableName), "materialized", timePredicate, timeColumn, dimensionRequired));
        }

        missingPreComputedTables.add(new CalculatedUserSet(collection, dimension));
        return Optional.empty();
    }

    private String generatePreCalculatedTableSql(
            Optional<String> tableNameSuffix,
            String schema, String timePredicate, String timeColumn,
            boolean dimensionRequired) {
        return String.format("select %s as date, %s _user_set from %s where date %s",
                String.format(timeColumn, "date"),
                dimensionRequired ? "dimension, " : "",
                "\"" + checkProject(schema, '"') + "\"" + tableNameSuffix.map(e -> ".\"" + e + "\"").orElse(""), timePredicate);
    }

    private String diffTimestamps(DateUnit dateUnit, String start, String end) {
        return String.format("date_diff('%s', %s, %s)",
                dateUnit.name().toLowerCase(), start, end);
    }

    protected String getTableSubQuery(
            boolean mappingEnabled,
            String collection,
            String connectorField,
            Optional<Boolean> isText,
            String timeColumn,
            Optional<String> dimension,
            LocalDate startDate,
            LocalDate endDate,
            Optional<Expression> filter) {
        String userField = isText.map(text -> String.format("%s", checkTableColumn(connectorField))).orElse(connectorField);
        String timePredicate = String.format("between date '%s' and date '%s' + interval '1' day",
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE));

        return format("select %s as date, %s %s from %s as data %s where data.%s %s %s",
                String.format(timeColumn, "data." + checkTableColumn(projectConfig.getTimeColumn())),
                dimension.isPresent() ? checkTableColumn(dimension.get(), "data.dimension", '"') + " as dimension, " : "",
                (userMappingEnabled && mappingEnabled) ? String.format("(case when data.%s is not null then data.%s else coalesce(mapping._user, data._device_id) end) as %s", userField, userField, userField) : ("data." + userField),
                checkCollection(collection),
                (userMappingEnabled && mappingEnabled) ? String.format("left join %s mapping on (data.%s is null and mapping.created_at >= date '%s' and mapping.merged_at <= date '%s' and mapping.id = data._user)",
                        checkCollection(ANONYMOUS_ID_MAPPING), checkTableColumn(projectConfig.getUserColumn()), startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE)) : "",
                checkTableColumn(projectConfig.getTimeColumn()),
                timePredicate,
                filter.isPresent() ? "and " + formatExpression(filter.get(), reference -> {
                    throw new UnsupportedOperationException();
                }, '"') : "");
    }
}

