package org.rakam.ui;

import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.Config;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.util.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 21:21.
 */
public class RakamUIModule extends RakamModule {
    final static Logger LOGGER = LoggerFactory.getLogger(RakamUIModule.class);

    private static final File UI_DIRECTORY = new File(System.getProperty("user.dir"), "_ui");

    @Override
    protected void setup(Binder binder) {
        RakamUIConfig rakamUIConfig = buildConfigObject(RakamUIConfig.class);
        if (rakamUIConfig.getUI() == null) {
            return;
        }

        URI uri;
        try {
            uri = new URI(rakamUIConfig.getUI());
        } catch (URISyntaxException e) {
            binder.addError(e);
            return;
        }

        File directory;

        if(uri.getScheme().equals("file")) {
            directory = new File(uri.getHost(), uri.getPath());
        } else {
            if (!UI_DIRECTORY.exists()) {
                UI_DIRECTORY.mkdirs();

                try {
                    Path dest = new File(UI_DIRECTORY, "ui.zip").toPath();
                    switch (uri.getScheme()) {
                        case "github":
                            String url;

                            String substring = uri.getPath().substring(1);
                            int i = substring.lastIndexOf(":");
                            String name;
                            String repo;
                            if (i > -1) {
                                name = substring.substring(i);
                                repo = substring.substring(0, i);
                                url = uri.getAuthority() + "/" + repo + "/archive/" + name + ".zip";
                            } else {
                                name = "master";
                                repo = substring;
                                url = uri.getAuthority() + "/" + substring + "/archive/" + name + ".zip";
                            }
                            try {
                                String spec = "https://github.com/" + url;
                                LOGGER.info("Downloading {} to {}.", spec, dest.toFile().getAbsolutePath());
                                new HttpDownloadHelper().download(new URL(spec),
                                        dest,
                                        new HttpDownloadHelper.VerboseProgress(System.out));

                                File file = new File(UI_DIRECTORY, "ui.zip");
                                extractFolder(file, UI_DIRECTORY);
                                file.delete();
                                directory = new File(UI_DIRECTORY, repo + "-" + name);
                            } catch (Exception e) {
                                return;
                            }
                            break;
                        case "file":
                            directory = new File(uri.getHost() + File.separator + uri.getPath().substring(1));
                            break;
                        case "http":
                        case "https":
                            new HttpDownloadHelper().download(new URL(rakamUIConfig.getUI()),
                                    dest,
                                    new HttpDownloadHelper.VerboseProgress(System.out));

                            File file = new File(UI_DIRECTORY, "ui.zip");
                            extractFolder(file, UI_DIRECTORY);
                            file.delete();
                            directory = UI_DIRECTORY;
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            } else {
                LOGGER.warn("_ui directory is not empty, skipping.");
                directory = UI_DIRECTORY;
            }
        }

        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);
        httpServices.addBinding().to(CustomReportHttpService.class);
        httpServices.addBinding().to(CustomPageHttpService.class);
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

    public static class RakamUIConfig {
        private String ui;
        private String directory;

        @Config("ui.source")
        public RakamUIConfig setUI(String ui) {
            this.ui = ui;
            return this;
        }

        public String getUI() {
            return ui;
        }

        @Config("ui.directory")
        public RakamUIConfig setDirectory(String directory) {
            this.directory = directory;
            return this;
        }

        public String getDirectory() {
            return directory;
        }
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
}
