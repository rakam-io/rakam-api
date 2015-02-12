package org.rakam.report.metadata;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.rakam.config.MetadataConfig;
import org.rakam.report.metadata.postgresql.PostgresqlMetadataConfig;
import org.rakam.report.metadata.postgresql.PostgresqlReportMetadata;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/02/15 18:01.
 */
public class ReportMetadataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        MetadataConfig config = buildConfigObject(MetadataConfig.class);

        switch (config.getStore().toLowerCase().trim()) {
            case "postgresql":
                PostgresqlReportMetadata metadata = new PostgresqlReportMetadata(buildConfigObject(PostgresqlMetadataConfig.class));
                binder.bind(ReportMetadataStore.class).toInstance(metadata);
        }
    }

}
