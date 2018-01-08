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
package org.rakam.analysis.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.SchemaField;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RemoteTable {
    public final URL url;
    public final boolean indexUrl;
    public final List<SchemaField> columns;
    public final CompressionType compressionType;
    public final ExternalSourceType format;
    public final Map<String, String> typeOptions;

    @JsonCreator
    public RemoteTable(
            @JsonProperty("url") URL url,
            @JsonProperty("indexUrl") Boolean indexUrl,
            @JsonProperty("typeOptions") Map<String, String> typeOptions,
            @JsonProperty("columns") List<SchemaField> columns,
            @JsonProperty("compressionType") CompressionType compressionType,
            @JsonProperty("format") ExternalSourceType format) {
        this.url = url;
        this.indexUrl = indexUrl == Boolean.TRUE;
        this.typeOptions = typeOptions;
        this.columns = requireNonNull(columns, "columns is null");
        this.compressionType = compressionType;
        this.format = requireNonNull(format, "format is null");
    }

    public enum CompressionType {
        GZIP
    }


    public enum ExternalSourceType {
        CSV, AVRO
    }
}