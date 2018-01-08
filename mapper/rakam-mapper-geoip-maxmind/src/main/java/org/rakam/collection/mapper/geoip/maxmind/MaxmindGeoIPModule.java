package org.rakam.collection.mapper.geoip.maxmind;

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.UserPropertyMapper;
import org.rakam.util.ConditionalModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

@AutoService(RakamModule.class)
@ConditionalModule(config = "plugin.geoip.enabled", value = "true")
public class MaxmindGeoIPModule
        extends RakamModule {
    static File downloadOrGetFile(URL url)
            throws Exception {
        if ("file".equals(url.getProtocol())) {
            return new File(url.toString().substring("file:/".length()));
        }
        String name = url.getFile().substring(url.getFile().lastIndexOf('/') + 1, url.getFile().length());
        File data = new File(new File(System.getProperty("java.io.tmpdir")), "rakam/" + name);

        data.getParentFile().mkdirs();

        String extension;
        if (url.getHost().equals("download.maxmind.com") && url.getPath().startsWith("/app")) {
            extension = "tar.gz";
        } else {
            extension = Files.getFileExtension(data.getAbsolutePath()).split("\\?")[0];
        }

        if (!data.exists()) {
            try {
                new HttpDownloadHelper().download(url, data.toPath(), new HttpDownloadHelper.VerboseProgress(System.out));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        File extractedFile = new File("/tmp/rakam/" + Files.getNameWithoutExtension(data.getAbsolutePath()) + "-extracted");
        if (extractedFile.exists() && extractedFile.length() > 0) {
            return extractedFile;
        }

        if (!extractedFile.getParentFile().exists()) {
            extractedFile.getParentFile().mkdirs();
        }

        if (extension.equals("tar.gz")) {
            TarArchiveInputStream tarInput =
                    new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(data)));

            FileOutputStream out = new FileOutputStream(extractedFile);
            try {
                ArchiveEntry entry;
                int size = -1;
                while (null != (entry = tarInput.getNextEntry())) {
                    if (entry.getName().endsWith("mmdb")) {
                        size = Ints.checkedCast(entry.getSize());
                        break;
                    } else {
                        ByteStreams.toByteArray(tarInput);
                    }
                }

                if (size == -1) {
                    throw new IllegalStateException("tar.gz file doesn't contain an mmdb file");
                }

                byte[] bytes = new byte[size];
                tarInput.read(bytes);
                out.write(bytes);
            } finally {
                tarInput.close();
                out.close();
                data.delete();
            }

            return extractedFile;
        } else {
            if (extension.equals("gz")) {
                GZIPInputStream gzipInputStream =
                        new GZIPInputStream(new FileInputStream(data));

                FileOutputStream out = new FileOutputStream(extractedFile);

                byte[] buffer = new byte[1024];
                int len;
                while ((len = gzipInputStream.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
                }

                gzipInputStream.close();
                out.close();
                data.delete();

                return extractedFile;
            } else if (extension.equals("mmdb")) {
                return data;
            } else {
                throw new IllegalArgumentException("Unknown extension of Maxming GeoIP file: " + extension);
            }
        }
    }

    @Override
    protected void setup(Binder binder) {
        MaxmindGeoIPModuleConfig geoIPModuleConfig = buildConfigObject(MaxmindGeoIPModuleConfig.class);
        MaxmindGeoIPEventMapper geoIPEventMapper;
        try {
            geoIPEventMapper = new MaxmindGeoIPEventMapper(geoIPModuleConfig);
        } catch (IOException e) {
            binder.addError(e);
            return;
        }
        Multibinder.newSetBinder(binder, UserPropertyMapper.class).addBinding().toInstance(geoIPEventMapper);
        Multibinder.newSetBinder(binder, EventMapper.class).addBinding().toInstance(geoIPEventMapper);
    }

    @Override
    public String name() {
        return "GeoIP Event Mapper";
    }

    @Override
    public String description() {
        return "It attaches the events that have ip attribute with location information by GeoIP lookup service.";
    }
}
