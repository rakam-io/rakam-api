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
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.FieldDependencyBuilder;

import java.net.InetAddress;
import java.time.Instant;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;

public class TimestampEventMapper implements EventMapper {
    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        GenericRecord properties = event.properties();
        Object time = properties.get("_time");
        if (time == null) {
            long serverTime = Instant.now().getEpochSecond();

            properties.put("_time", serverTime * 1000);
        } else {
            if (time instanceof Number) {
                // match server time and client time and get an estimate
                if (event.api() != null && event.api().uploadTime != null) {
                    long fixedTime = ((Number) time).longValue() + Instant.now().getEpochSecond() - event.api().uploadTime;
                    properties.put("_time", fixedTime * 1000);
                }
            }
        }
        return null;
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields(of(new SchemaField("_time", FieldType.TIMESTAMP)));
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
