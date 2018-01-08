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
package org.rakam.ui.customreport;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


public class CustomReport {
    public final String reportType;
    public final Integer user;
    public final String userEmail;
    public final String name;
    public final Object data;

    @JsonCreator
    public CustomReport(@JsonProperty(value = "report_type", required = true) String reportType,
                        @JsonProperty(value = "name", required = true) String name,
                        @JsonProperty(value = "data", required = true) Object data) {
        this(reportType, name, null, null, data);
    }

    public CustomReport(String reportType, String name, Integer user, String userEmail, Object data) {
        this.reportType = reportType;
        this.user = user;
        this.userEmail = userEmail;
        this.name = name;
        this.data = data;
    }

    @JsonProperty("report_type")
    public String getReportType() {
        return reportType;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("data")
    public Object getData() {
        return data;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("user")
    public Integer getUser() {
        return user;
    }
}
