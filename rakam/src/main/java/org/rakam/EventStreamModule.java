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
package org.rakam;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.analysis.stream.EventStreamHttpService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventStreamConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/08/15 15:17.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config="event.stream.enabled", value = "true")
public class EventStreamModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        if (buildConfigObject(EventStreamConfig.class).isEventStreamEnabled()) {
            Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
            httpServices.addBinding().to(EventStreamHttpService.class);
        }
    }

    @Override
    public String name() {
        return "Event stream module";
    }

    @Override
    public String description() {
        return null;
    }
}
