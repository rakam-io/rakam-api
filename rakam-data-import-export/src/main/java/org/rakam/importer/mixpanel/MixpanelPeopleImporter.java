package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.rakam.ApiClient;
import org.rakam.ApiException;
import org.rakam.auth.ApiKeyAuth;
import org.rakam.client.api.EventApi;
import org.rakam.client.model.BatchEvents;
import org.rakam.collection.SchemaField;
import org.rakam.importer.RakamClient;
import org.rakam.util.JsonHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Map;

@Command(name = "import-people", description = "Mixpanel importer")
public class MixpanelPeopleImporter implements Runnable {
    @Option(name="--mixpanel.api-key", description = "Api key", required = true)
    public String apiKey;

    @Option(name="--mixpanel.api-secret", description = "Api secret", required = true)
    public String apiSecret;

    @Option(name="--rakam.address", description = "Rakam cluster url", required = true)
    public String rakamAddress;

    @Option(name="--rakam.project", description = "Rakam cluster url", required = true)
    public String rakamProject;

    @Option(name="--mixpanel.project.timezone", description = "Rakam cluster url", required = true)
    public Integer projectTimezone;

    @Option(name="--rakam.project.write-key", description = "Project", required = true)
    public String rakamWriteKey;

    @Option(name="--schema.file", description = "Mixpanel people schema file")
    public File schemaFile;

    @Option(name="--schema", description = "Mixpanel people schema")
    public String schema;

    @Option(name="--last-seen", description = "Mixpanel people lastSeen filter as date (YYYY-mm-dd)")
    public String lastSeen;

    @Override
    public void run() {
        ApiClient apiClient = new ApiClient();
        apiClient.setApiKey(apiKey);
        ((ApiKeyAuth) apiClient.getAuthentication("write_key")).setApiKey(rakamWriteKey);

        BatchEvents events = new BatchEvents();
        try {
            new EventApi(apiClient).batchEvents(events);
        } catch (ApiException e) {
            e.printStackTrace();
        }

        MixpanelImporter mixpanelEventImporter = new MixpanelImporter(apiKey, apiSecret);

        if(schemaFile != null) {
            if(schema != null) {
                throw new IllegalArgumentException("Only one of schema and schemaFile can be set");
            }

            try {
                schema = new String(Files.readAllBytes(Paths.get(schemaFile.getPath())), "UTF-8");
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        Map<String, SchemaField> fields;
        if(schema != null) {
            fields = JsonHelper.read(schema, new TypeReference<Map<String, SchemaField>>() {});
        } else {
            fields = null;
        }

        LocalDate lastSeenDate;
        if(lastSeen != null) {
            lastSeenDate = LocalDate.parse(lastSeen);
        } else {
            lastSeenDate = null;
        }

        RakamClient client = new RakamClient(rakamAddress);

        mixpanelEventImporter.importPeopleFromMixpanel(rakamProject, fields, lastSeenDate,
                (users) -> client.setUserProperty(new RakamClient.UserList(rakamProject, users)));
    }


}
