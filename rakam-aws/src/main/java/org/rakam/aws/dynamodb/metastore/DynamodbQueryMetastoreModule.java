package org.rakam.aws.dynamodb.metastore;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.aws.AWSConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "query-metadata-store.adapter", value = "dynamodb")
public class DynamodbQueryMetastoreModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(DynamodbQueryMetastoreConfig.class);
        configBinder(binder).bindConfig(AWSConfig.class);

        binder.bind(QueryMetadataStore.class).to(DynamodbQueryMetastore.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }
}
