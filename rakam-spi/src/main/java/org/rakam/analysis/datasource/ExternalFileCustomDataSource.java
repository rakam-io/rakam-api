package org.rakam.analysis.datasource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.Boolean.FALSE;
import static org.rakam.analysis.datasource.RemoteTable.ExternalSourceType.AVRO;
import static org.rakam.analysis.datasource.RemoteTable.ExternalSourceType.CSV;

public class ExternalFileCustomDataSource {
    public static List<SchemaField> fillColumnIfNotSet(Map<String, String> typeOptions, RemoteTable.ExternalSourceType format, URL url, boolean indexUrl) {
        if (format == CSV && !FALSE.toString().equals(typeOptions.get("use_header"))) {
            URL file;
            try {
                file = getFile(url, indexUrl);
            } catch (RuntimeException e) {
                throw new RakamException(e.getMessage(), BAD_REQUEST);
            }

            String separator = Optional.ofNullable(typeOptions.get("column_separator")).orElse(",");

            try {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file.openStream()));

                ImmutableList.Builder<SchemaField> builder = ImmutableList.builder();
                for (String column : Splitter.on(separator).split(bufferedReader.readLine())) {
                    builder.add(new SchemaField(column, FieldType.STRING));
                }

                return builder.build();
            } catch (IOException e) {
                throw new RakamException("Error while parsing CSV: " + e.getMessage(), BAD_REQUEST);
            }
        }

        throw new RakamException("columns parameter is required", BAD_REQUEST);
    }

    private static URL getFile(URL url, boolean indexUrl)
            throws RuntimeException {
        if (!url.getProtocol().equals("http") && !url.getProtocol().equals("https") && !url.getProtocol().equals("ftp")) {
            throw new RuntimeException("URL is not valid. Use http, https or ftp schemes");
        }

        URL testUrl = url;
        if (indexUrl) {
            List<String> urls;
            try {
                urls = JsonHelper.read(ByteStreams.toByteArray(url.openStream()),
                        new TypeReference<List<String>>() {
                        });
            } catch (IOException e) {
                throw new RuntimeException("The index file must be an array containing urls. Example: [\"http://myurl.com/a.csv\"]");
            }
            if (urls == null || urls.isEmpty()) {
                throw new RuntimeException("Index file doesn't have any entry");
            }

            try {
                testUrl = new URL(urls.get(0));
            } catch (MalformedURLException e) {
                throw new RuntimeException("Index file doesn't contain URL values");
            }
        }

        return testUrl;
    }

    public static Optional<String> test(RemoteTable remoteTable) {
        try {
            URL testUrl;
            try {
                testUrl = getFile(remoteTable.url, remoteTable.indexUrl);
            } catch (RuntimeException e) {
                return Optional.of(e.getMessage());
            }

            if (remoteTable.format == CSV) {
                String line;
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(testUrl.openStream()));
                int i = 0;
                try {
                    while (((line = bufferedReader.readLine()) != null) && i < 5) {
                        if (line.isEmpty()) {
                            // TODO: better alternative to detect csv
                            return Optional.of("There are empty lines");
                        }
                        i++;
                    }
                } finally {
                    bufferedReader.close();
                }
            } else if (remoteTable.format == AVRO) {
                try {
                    testUrl.openStream().read();
                } catch (IOException e) {
                    return Optional.of("Unable to read data from server");
                }
            } else {
                throw new IllegalStateException();
            }

            return Optional.empty();
        } catch (IOException e) {
            return Optional.of(e.getMessage());
        }
    }
}
