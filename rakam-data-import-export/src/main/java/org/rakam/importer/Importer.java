package org.rakam.importer;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import org.rakam.importer.mixpanel.MixpanelEventExplainer;
import org.rakam.importer.mixpanel.MixpanelEventImporter;
import org.rakam.importer.mixpanel.MixpanelPeopleExplainer;
import org.rakam.importer.mixpanel.MixpanelPeopleImporter;

public class Importer {
    public static void main(String[] args) {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("import")
                .withDescription("Rakam importer")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class);

        builder.withGroup("mixpanel")
                .withDefaultCommand(Help.class)
                .withCommands(MixpanelEventImporter.class, MixpanelEventExplainer.class, MixpanelPeopleImporter.class, MixpanelPeopleExplainer.class);

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }
}
