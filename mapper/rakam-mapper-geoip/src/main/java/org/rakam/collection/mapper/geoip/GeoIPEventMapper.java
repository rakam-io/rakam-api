package org.rakam.collection.mapper.geoip;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Country;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Created by buremba on 26/05/14.
 */
public class GeoIPEventMapper implements EventMapper {
    final static Logger LOGGER = LoggerFactory.getLogger(GeoIPEventMapper.class);

    private final static List<String> ATTRIBUTES = ImmutableList.of("country","countryCode","region","city","latitude","longitude","timezone");
    DatabaseReader countryLookup;
    String[] attributes;

    private File downloadOrGetFile(String fileUrl) throws Exception {
        URL url = new URL(fileUrl);
        String name = url.getFile().substring(url.getFile().lastIndexOf('/') + 1, url.getFile().length());
        File data = new File("data/" + name);
        data.getParentFile().mkdir();

        String extension = Files.getFileExtension(data.getAbsolutePath());
        if(extension.equals("gz")) {
            File extractedFile = new File("data/" + Files.getNameWithoutExtension(data.getAbsolutePath()));
            if(extractedFile.exists()) {
                return extractedFile;
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
            if(data.exists()) {
                return data;
            }

            new HttpDownloadHelper().download(url, data.toPath(), new HttpDownloadHelper.VerboseProgress(System.out));

            return data;
        }
    }

    public GeoIPEventMapper(GeoIPModuleConfig config) throws IOException {
        Preconditions.checkNotNull(config, "config is null");
        InputStream countryDatabase;
        if(config.getDatabase() != null) {
            countryDatabase = new FileInputStream(config.getDatabase());
        } else
        if(config.getDatabaseUrl() != null) {
            try {
                countryDatabase = new FileInputStream(downloadOrGetFile(config.getDatabaseUrl()));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            throw new IllegalStateException();
        }

        countryLookup = new DatabaseReader.Builder(countryDatabase).fileMode(Reader.FileMode.MEMORY).build();
        if(config.getAttributes() != null) {
            for (String attr : config.getAttributes()) {
                if(!ATTRIBUTES.contains(attr)) {
                    throw new IllegalArgumentException("Attribute "+attr+" is not exist. Available attributes: " +
                            Joiner.on(", ").join(ATTRIBUTES));
                }
            }
            attributes = config.getAttributes().stream().toArray(String[]::new);
        } else {
            attributes = ATTRIBUTES.toArray(new String[ATTRIBUTES.size()]);
        }
    }

    @Override
    public void map(Event event) {
        GenericRecord properties = event.properties();
        Object IP = properties.get("ip");
        InetAddress ipAddress;
        try {
            if(!(IP instanceof String)) {
                return;
            }
            ipAddress = InetAddress.getByName((String) IP);
            if(ipAddress == null) {
                return;
            }
        } catch (UnknownHostException e) {
            return;
        }

        // TODO: lazy initialization
        CityResponse response;
        try {
            response = countryLookup.city(ipAddress);
        } catch (AddressNotFoundException e) {
            return;
        }catch (Exception e) {
            LOGGER.error("Error while search for location information. ", e);
            return;
        }

//        countryLookup.isp()
//        countryLookup.connectionType()

        Country country = response.getCountry();

        for (String attribute : attributes) {
            switch (attribute) {
                case "country":
                    properties.put("country", country.getName());
                    break;
                case "countryCode":
                    properties.put("countryCode", country.getIsoCode());
                    break;
                case "region":
                    properties.put("region", response.getContinent().getName());
                    break;
                case "city":
                    properties.put("city", response.getCity().getName());
                    break;
                case "latitude":
                    properties.put("latitude", response.getLocation().getLatitude());
                    break;
                case "longitude":
                    properties.put("longitude", response.getLocation().getLongitude());
                    break;
                case "timezone":
                    properties.put("timezone", response.getLocation().getTimeZone());
                    break;
            }
        }
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields("ip", Arrays.stream(attributes)
                .map(attr -> new SchemaField(attr, getType(attr), true))
                .collect(Collectors.toList()));
    }

    private FieldType getType(String attr) {
        switch (attr) {
            case "country":
            case "countryCode":
            case "region":
            case "city":
            case "timezone":
                return FieldType.STRING;
            case "latitude":
            case "longitude":
                return FieldType.DOUBLE;
            default:
                throw new IllegalStateException();
        }
    }


}
