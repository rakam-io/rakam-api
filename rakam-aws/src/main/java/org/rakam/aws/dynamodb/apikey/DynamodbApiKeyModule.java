package org.rakam.aws.dynamodb.apikey;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import org.rakam.aws.AWSConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "api-key-service.adapter", value = "dynamodb")
public class DynamodbApiKeyModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(DynamodbApiKeyConfig.class);
        configBinder(binder).bindConfig(AWSConfig.class);
    }

    @Override
    public String name() {
        return "Dynamodb Api Key Service";
    }

    @Override
    public String description() {
        return null;
    }
}
