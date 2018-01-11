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

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.Mapper;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;

import javax.inject.Inject;
import java.net.InetAddress;
import java.time.Instant;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;

@Mapper(name = "Timestamp mapper", description = "Attaches or re-configures time attribute of events.")
public class TimestampEventMapper
        implements SyncEventMapper {
    private static final int HASHCODE = TimestampEventMapper.class.getName().hashCode();
    private final ProjectConfig projectConfig;

    @Inject
    public TimestampEventMapper(ProjectConfig projectConfig) {
        this.projectConfig = projectConfig;
    }

    @Override
    public List<Cookie> map(Event event, RequestParams extraProperties, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        GenericRecord properties = event.properties();
        Object time = properties.get(projectConfig.getTimeColumn());
        if (time == null) {
            long serverTime = Instant.now().getEpochSecond();

            properties.put(projectConfig.getTimeColumn(), serverTime * 1000);
        } else if (time instanceof Number && event.api() != null && event.api().uploadTime != null) {
            // match server time and client time and get an estimate
            long fixedTime = ((Number) time).longValue() + ((Instant.now().getEpochSecond() - (event.api().uploadTime / 1000)) * 1000);
            properties.put(projectConfig.getTimeColumn(), fixedTime);
        }
        return null;
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields(of(new SchemaField(projectConfig.getTimeColumn(), FieldType.TIMESTAMP)));
    }

    @Override
    public int hashCode() {
        return HASHCODE;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TimestampEventMapper;
    }
}
