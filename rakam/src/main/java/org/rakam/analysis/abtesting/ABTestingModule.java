package org.rakam.analysis.abtesting;

import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.swagger.models.Tag;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

@ConditionalModule(config = "event.ab-testing.enabled", value = "true")
public class ABTestingModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ABTestingHttpService.class);

        Multibinder<Tag> tags = Multibinder.newSetBinder(binder, Tag.class);
        tags.addBinding().toInstance(new Tag().name("ab-testing").description("A/B Testing Module").externalDocs(MetadataConfig.centralDocs));
    }

    @Override
    public String name() {
        return "A/B Testing Module";
    }

    @Override
    public String description() {
        return "A/B Testing with your event data";
    }
}
