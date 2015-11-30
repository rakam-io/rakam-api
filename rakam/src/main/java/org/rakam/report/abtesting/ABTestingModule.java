package org.rakam.report.abtesting;

import com.google.auto.service.AutoService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;

@AutoService(RakamModule.class)
@ConditionalModule(config="event.ab-testing.enabled", value = "true")
public class ABTestingModule {
}
