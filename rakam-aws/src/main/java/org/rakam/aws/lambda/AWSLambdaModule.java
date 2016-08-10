package org.rakam.aws.lambda;

import com.google.inject.Binder;
import org.rakam.plugin.RakamModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

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

    public static void main(String[] args)
    {

    }
}
