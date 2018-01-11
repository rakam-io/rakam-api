package org.rakam.aws.dynamodb.config;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import org.rakam.analysis.ConfigManager;
import org.rakam.aws.AWSConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "config-manager.adapter", value = "dynamodb")
public class DynamodbConfigManagerModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(DynamodbConfigManagerConfig.class);
        configBinder(binder).bindConfig(AWSConfig.class);

        binder.bind(ConfigManager.class).to(DynamodbConfigManager.class);
    }

    @Override
    public String name() {
        return "Dynamodb Config Manager";
    }

    @Override
    public String description() {
        return null;
    }
}
