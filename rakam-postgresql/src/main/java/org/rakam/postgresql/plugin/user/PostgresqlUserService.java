package org.rakam.postgresql.plugin.user;

import com.google.common.collect.ImmutableMap;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.ISingleUserBatchOperation;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.of;

public class PostgresqlUserService extends AbstractUserService {
    public static final String ANONYMOUS_ID_MAPPING = "$anonymous_id_mapping";
    protected static final Map<FieldType, List<SchemaField>> ANONYMOUS_USER_MAPPING = ImmutableMap.of(
            FieldType.STRING, of(
                    new SchemaField("id", FieldType.STRING),
                    new SchemaField("_user", FieldType.STRING),
                    new SchemaField("created_at", FieldType.TIMESTAMP),
                    new SchemaField("merged_at", FieldType.TIMESTAMP)),

            FieldType.LONG, of(
                    new SchemaField("id", FieldType.STRING),
                    new SchemaField("_user", FieldType.STRING),
                    new SchemaField("created_at", FieldType.TIMESTAMP),
                    new SchemaField("merged_at", FieldType.TIMESTAMP)),

            FieldType.INTEGER, of(
                    new SchemaField("id", FieldType.STRING),
                    new SchemaField("_user", FieldType.STRING),
                    new SchemaField("created_at", FieldType.TIMESTAMP),
                    new SchemaField("merged_at", FieldType.TIMESTAMP))
    );
    private final PostgresqlUserStorage storage;

    @Inject
    public PostgresqlUserService(PostgresqlUserStorage storage) {
        super(storage);
        this.storage = storage;
    }

    @Override
    public CompletableFuture<Void> batch(String project, List<? extends ISingleUserBatchOperation> batchUserOperations) {
        return storage.batch(project, batchUserOperations);
    }
}
