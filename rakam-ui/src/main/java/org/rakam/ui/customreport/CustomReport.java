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
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;


public class CustomReport implements ProjectItem {
    public final String reportType;
    public final String project;
    public final String name;
    public final Object data;

    @JsonCreator
    public CustomReport(@ApiParam(name="report_type") String reportType,
                        @ApiParam(name="project") String project,
                        @ApiParam(name="name") String name,
                        @ApiParam(name="data") Object data) {
        this.reportType = reportType;
        this.project = project;
        this.name = name;
        this.data = data;
    }

    @Override
    public String project() {
        return project;
    }
}
