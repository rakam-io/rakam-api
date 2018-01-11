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
package org.rakam.analysis.funnel;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.swagger.models.Tag;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

@AutoService(RakamModule.class)
@ConditionalModule(config = "user.funnel-analysis.enabled", value = "true")
public class FunnelAnalyzerModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(FunnelAnalyzerHttpService.class);

        Multibinder<Tag> tags = Multibinder.newSetBinder(binder, Tag.class);
        tags.addBinding().toInstance(new Tag().name("funnel").description("Funnel Analyzer").externalDocs(MetadataConfig.centralDocs));
    }

    @Override
    public String name() {
        return "Funnel Analyzer Module";
    }

    @Override
    public String description() {
        return "Analyzes event data and creates funnels of user activities.";
    }
}
