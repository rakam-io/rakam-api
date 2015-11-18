package org.rakam.importer.mixpanel;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.rakam.util.JsonHelper;

@Command(name = "explain-events", description = "Mixpanel importer")
public class MixpanelEventExplainer implements Runnable {
    @Option(name="--mixpanel.api-key", description = "Api key", required = true)
    public String apiKey;

    @Option(name="--mixpanel.api-secret", description = "Api secret", required = true)
    public String apiSecret;

    @Override
    public void run() {
        MixpanelImporter mixpanel = new MixpanelImporter(apiKey, apiSecret);
        System.out.println(JsonHelper.encode(mixpanel.mapEventFields(), true));
    }
}
