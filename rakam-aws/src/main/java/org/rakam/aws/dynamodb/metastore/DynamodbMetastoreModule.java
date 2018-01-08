package org.rakam.aws.dynamodb.metastore;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.AWSConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "metastore.adapter", value = "dynamodb")
public class DynamodbMetastoreModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(DynamodbMetastoreConfig.class);
        configBinder(binder).bindConfig(AWSConfig.class);

        binder.bind(Metastore.class).to(DynamodbMetastore.class);
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