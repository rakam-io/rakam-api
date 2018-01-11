package org.rakam.analysis.suggestion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.SystemEvents;
import org.rakam.util.NotExistsException;

import javax.inject.Inject;
import java.time.Duration;
import java.util.List;

import static org.rakam.util.ValidationUtil.*;

public class AttributeHook {
    public static final String TABLE_NAME = "_attribute_unique_values";
    private static List<FieldType> SUPPORTED_TYPES = ImmutableList.of(FieldType.STRING);
    private final MaterializedViewService materializedViewService;
    private final ProjectConfig projectConfig;
    private final Metastore metastore;

    @Inject
    public AttributeHook(ProjectConfig projectConfig, Metastore metastore, MaterializedViewService materializedViewService) {
        this.materializedViewService = materializedViewService;
        this.projectConfig = projectConfig;
        this.metastore = metastore;
    }

    @Subscribe
    public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
        internal(event.project, event.collection, event.fields);
    }

    @Subscribe
    public void onCreateAttribute(SystemEvents.CollectionFieldCreatedEvent event) {
        internal(event.project, event.collection, event.fields);
    }

    public void add(String project, String collection) {
        internal(project, collection, metastore.getCollection(project, collection));
    }

    private void internal(String project, String collection, List<SchemaField> fields) {
        String query;
        boolean isNew;
        MaterializedView view;
        try {
            view = materializedViewService.get(project, TABLE_NAME);
            query = view.query;
            isNew = false;
        } catch (NotExistsException e) {
            view = new MaterializedView(TABLE_NAME, "Unique attribute values", "select 1", Duration.ofDays(1), true, false, ImmutableMap.of());
            query = "";
            isNew = true;
        }

        boolean firstIteration = isNew;
        for (SchemaField field : fields) {
            if (!SUPPORTED_TYPES.contains(field.getType()) || field.getName().equals(projectConfig.getUserColumn())) {
                continue;
            }

            if (!firstIteration) {
                query += " union all ";
            }

            query += String.format("select * from (\n" +
                            "\tselect cast(%s as date) as date, '%s' as collection, '%s' as attribute, %s as value from %s group by 1, 2, 3, 4 order by count(*) limit 10000\n" +
                            ") \"unique_values_%s_%s\"", projectConfig.getTimeColumn(),
                    checkLiteral(collection),
                    checkLiteral(field.getName()),
                    checkTableColumn(field.getName()),
                    checkCollection(collection), checkCollectionValid(collection),
                    stripName(field.getName(), "attribute"));
            firstIteration = false;
        }

        if (query.isEmpty()) {
            return;
        }

        view = new MaterializedView(view.tableName, view.name, query, view.updateInterval, view.incremental, view.realTime, view.options);
        if (isNew) {
            materializedViewService.create(new RequestContext(project, null), view);
        } else {
            materializedViewService.replaceView(project, view);
        }
    }
}
