package org.rakam.collection.mapper.geoip.maxmind.ip2location;

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
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
@ConditionalModule(config = "plugin.geoip.ip2location", value = "true")
public class IP2LocationGeoIPModule
        extends RakamModule {
    static File downloadOrGetFile(String fileUrl) throws Exception {
        URL url = new URL(fileUrl);
        if ("file".equals(url.getProtocol())) {
            return new File(fileUrl.substring("file:/".length()));
        }
        String name = url.getFile().substring(url.getFile().lastIndexOf('/') + 1, url.getFile().length());
        File data = new File(new File(System.getProperty("java.io.tmpdir")), "rakam/" + name);

        data.getParentFile().mkdirs();

        String extension = Files.getFileExtension(data.getAbsolutePath());
        if (extension.equals("gz")) {
            File extractedFile = new File("/tmp/rakam/" + Files.getNameWithoutExtension(data.getAbsolutePath()));
            if (extractedFile.exists()) {
                return extractedFile;
            }

            if (!extractedFile.getParentFile().exists()) {
                extractedFile.getParentFile().mkdirs();
            }

            if (!data.exists()) {
                try {
                    new HttpDownloadHelper().download(url, data.toPath(), new HttpDownloadHelper.VerboseProgress(System.out));
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }

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
        } else {
            if (data.exists()) {
                return data;
            }

            new HttpDownloadHelper().download(url, data.toPath(), new HttpDownloadHelper.VerboseProgress(System.out));

            return data;
        }
    }

    @Override
    protected void setup(Binder binder) {
        GeoIPModuleConfig geoIPModuleConfig = buildConfigObject(GeoIPModuleConfig.class);
        IP2LocationGeoIPEventMapper geoIPEventMapper;
        try {
            geoIPEventMapper = new IP2LocationGeoIPEventMapper(geoIPModuleConfig);
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

