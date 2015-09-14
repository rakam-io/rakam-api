package org.rakam.ui;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.binder.AnnotatedBindingBuilder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEventListener;
import org.rakam.server.http.HttpService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 21:21.
 */
public class RakamUIModule extends RakamModule {
    final static Logger LOGGER = LoggerFactory.getLogger(RakamUIModule.class);

    @Override
    protected void setup(Binder binder) {
        RakamUIConfig rakamUIConfig = buildConfigObject(RakamUIConfig.class);
        bindConfig(binder)
                .annotatedWith(Names.named("ui.metadata.store.jdbc"))
                .prefixedWith("ui.metadata.store.jdbc")
                .to(JDBCConfig.class);

        AnnotatedBindingBuilder<CustomPageDatabase> customPageDb = binder.bind(CustomPageDatabase.class);
        switch (rakamUIConfig.getCustomPageBackend()) {
            case FILE:
                customPageDb.to(FileBackedCustomPageDatabase.class).in(Scopes.SINGLETON);
                break;
            case JDBC:
                customPageDb.to(JDBCCustomPageDatabase.class).in(Scopes.SINGLETON);
                break;
        }

        Multibinder.newSetBinder(binder, SystemEventListener.class)
                .addBinding().to(DefaultDashboardCreator.class);

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);
        httpServices.addBinding().to(CustomReportHttpService.class);
        httpServices.addBinding().to(CustomPageHttpService.class);
        httpServices.addBinding().to(DashboardService.class);
        httpServices.addBinding().to(RakamUIWebService.class);
    }

    @Override
    public String name() {
        return "Web Interface for Rakam APIs";
    }

    @Override
    public String description() {
        return "Can be used as a BI tool and a tool that allows you to create your customized analytics service frontend.";
    }

    public enum CustomPageBackend {
        FILE, JDBC
    }

    private void extractFolder(File file, File extractFolder) throws IOException {
        int BUFFER = 2048;

        ZipFile zip = new ZipFile(file);

        extractFolder.mkdir();
        Enumeration zipFileEntries = zip.entries();

        // Process each entry
        while (zipFileEntries.hasMoreElements()) {
            // grab a zip file entry
            ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
            String currentEntry = entry.getName();

            File destFile = new File(extractFolder, currentEntry);
            //destFile = new File(newPath, destFile.getName());
            File destinationParent = destFile.getParentFile();

            // create the parent directory structure if needed
            destinationParent.mkdirs();

            if (!entry.isDirectory()) {
                BufferedInputStream is = new BufferedInputStream(zip
                        .getInputStream(entry));
                int currentByte;
                // establish buffer for writing file
                byte data[] = new byte[BUFFER];

                // write the current file to disk
                FileOutputStream fos = new FileOutputStream(destFile);
                BufferedOutputStream dest = new BufferedOutputStream(fos,
                        BUFFER);

                // read and write until last byte is encountered
                while ((currentByte = is.read(data, 0, BUFFER)) != -1) {
                    dest.write(data, 0, currentByte);
                }
                dest.flush();
                dest.close();
                is.close();
            }
        }
    }

    public static class DefaultDashboardCreator implements SystemEventListener {

        private final DashboardService service;

        @Inject
        public DefaultDashboardCreator(DashboardService service) {
            this.service = service;
        }

        @Override
        public void onCreateProject(String project) {
            service.create(project, "Default");
        }
    }
}
