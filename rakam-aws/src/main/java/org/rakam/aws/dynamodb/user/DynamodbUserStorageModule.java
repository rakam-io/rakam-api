package org.rakam.aws.dynamodb.user;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.UserStorage;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "plugin.user.storage", value = "dynamodb")
public class DynamodbUserStorageModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(DynamodbUserConfig.class);
        binder.bind(UserStorage.class).to(DynamodbUserStorage.class)
                .in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "Dynamodb backend for user storage";
    }

    @Override
    public String description() {
        return "Dynamodb user storage backend for basic CRUD and search operations.";
    }
}
