package org.rakam.aws.lambda;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

@ConditionalModule(config = "tasks.enable", value = "true")
@AutoService(RakamModule.class)
public class AWSLambdaModule extends RakamModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AWSLambdaConfig.class);
    }

    @Override
    public String name()
    {
        return null;
    }

    @Override
    public String description()
    {
        return null;
    }
}
