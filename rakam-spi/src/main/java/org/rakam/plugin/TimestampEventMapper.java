/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.plugin;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.util.RakamException;

import java.net.InetAddress;
import java.time.Instant;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;

public class TimestampEventMapper implements EventMapper {
    private static final String TIME_EXTRA_PROPERTY = "Upload-Time";

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        GenericRecord properties = event.properties();
        Object time = properties.get("_time");
        if (time == null) {
            long serverTime = Instant.now().getEpochSecond();

            properties.put("_time", serverTime);
        } else {
            String stringValue = extraProperties.get(TIME_EXTRA_PROPERTY);
            long clientUploadTime;
            try {
                clientUploadTime = Long.parseLong(stringValue);
            } catch (NumberFormatException e) {
                throw new RakamException("Time checksum 'Upload-Time' has invalid value", HttpResponseStatus.UNAUTHORIZED);
            }
            if (time instanceof Number) {
                long serverTime = Instant.now().getEpochSecond();

                // match server time and client time and get an estimate
                long gap = serverTime - clientUploadTime;
                properties.put("_time", ((Number) time).longValue() + gap);
            }
        }
        return null;
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields(of(new SchemaField("_time", FieldType.LONG, false)));
    }

    @Override
    public int hashCode() {
        return HASHCODE;
    }

    private static final int HASHCODE = TimestampEventMapper.class.getName().hashCode();

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TimestampEventMapper;
    }
}
