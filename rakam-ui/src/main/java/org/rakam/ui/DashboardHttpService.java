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
package org.rakam.ui;

import com.google.inject.Inject;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;

import javax.ws.rs.Path;

/**
 * Created by buremba <Burak Emre Kabakcı> on 12/09/15 18:07.
 */
@Path("/ui/dashboard")
@Api(value = "/ui/dashboard", description = "Dashboard service", tags = "rakam-ui")
public class DashboardHttpService extends HttpService {

    @Inject
    public DashboardHttpService(DashboardService service) {
    }
}
