package org.rakam.presto.analysis;

import com.facebook.presto.raptor.metadata.MetadataDao;
import com.google.common.eventbus.EventBus;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.SchemaField;
import org.skife.jdbi.v2.DBI;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;

public class PrestoMetastore extends JDBCMetastore {

    private final DBI dbi;
    private final MetadataDao dao;

    @Inject
    public PrestoMetastore(@Named("presto.metastore.jdbc") JDBCPoolDataSource dataSource, PrestoConfig config, EventBus eventBus, FieldDependencyBuilder.FieldDependency fieldDependency) {
        super(dataSource, config, eventBus, fieldDependency);
        dbi = new DBI(JDBCPoolDataSource.getOrCreateDataSource(null));
        this.dao = onDemandDao(dbi, MetadataDao.class);
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> fields) {
        return null;
    }

    @Override
    public Map<String, Set<String>> getAllCollections() {
        return null;
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        return null;
//        return dao.listTableColumns(project, null).stream().collect(Collectors.toMap(col -> col.getTable().getTableName(),
//                col -> {
//                    List<TypeSignature> collect = col.getDataType().getTypeSignature().getParameters().stream().map(t -> new TypeSignature(t.getBase(), ImmutableList.of(), t.getLiteralParameters())).collect(Collectors.toList());
//                    return null;
////                    return new SchemaField(col.getColumnName(), fromPrestoType(new ClientTypeSignature(new TypeSignature(col.getDataType().getTypeSignature().getBase(), collect), col.getDataType().getTypeSignature().getLiteralParameters())));
//                }));
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        return null;
//        return dao.listTables(project).stream()
//                .map(a -> a.getTableName()).collect(Collectors.toSet());
    }

    @Override
    public void createProject(String project) {

    }

    @Override
    public Set<String> getProjects() {
        return dao.listSchemaNames().stream().collect(Collectors.toSet());
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        return null;
    }

}
