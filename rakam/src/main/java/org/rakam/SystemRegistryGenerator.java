package org.rakam;

import com.google.inject.Module;
import org.rakam.bootstrap.SystemRegistry;
import org.rakam.bootstrap.SystemRegistry.ModuleDescriptor.Condition;
import org.rakam.util.JsonHelper;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Locale;
import java.util.Set;

public final class SystemRegistryGenerator {

    private SystemRegistryGenerator() throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static void main(String[] args) throws IOException {
        try {
            if (args.length != 1 || !args[0].equals("json") && !args[0].equals("properties")) {
                System.err.println("Usage: [json] or [properties]");
                System.exit(1);
            }
            Set<Module> allModules = ServiceStarter.getModules();
            SystemRegistry systemRegistry = new SystemRegistry(allModules, allModules);
            if (args[0].equals("json")) {
                System.out.print(JsonHelper.encode(systemRegistry));
            } else {

                PrintWriter printWriter = new PrintWriter(System.out);

                for (SystemRegistry.ModuleDescriptor moduleDescriptor : systemRegistry.getModules()) {
                    String name;
                    if (moduleDescriptor.name == null) {
                        name = moduleDescriptor.className;
                    } else {
                        name = moduleDescriptor.name;
                    }

                    printWriter.println("#------------------------------------------------------------------------------");
                    printWriter.println("#" + name.toUpperCase(Locale.ENGLISH));
                    if (moduleDescriptor.description != null) {
                        printWriter.println("#" + moduleDescriptor.description);
                    }
                    printWriter.println("#------------------------------------------------------------------------------");

                    if (moduleDescriptor.condition.isPresent()) {
                        Condition condition = moduleDescriptor.condition.get();
                        printWriter.println("#Condition for this plugin to be is_active:");

                        if (condition.expectedValue.isEmpty()) {
                            printWriter.println("#" + condition.property + " property must be set");
                        } else {
                            printWriter.println("#" + condition.property + "=" + condition.expectedValue + "\n");
                        }
                    }

                    for (SystemRegistry.ConfigItem property : moduleDescriptor.properties) {
                        if (property.description != null && !property.description.isEmpty()) {
                            // TODO: support for breaking words to multiple lines
                            printWriter.println("# " + property.description);
                        }
                        String value = property.defaultValue.equals("null") ? "" : property.defaultValue;
                        printWriter.println("#" + property.property + "=" + value);
                    }

                    printWriter.print("\n\n");
                }
                printWriter.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
