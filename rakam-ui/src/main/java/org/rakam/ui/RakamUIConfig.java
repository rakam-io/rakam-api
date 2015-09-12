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

import io.airlift.configuration.Config;

import java.io.File;
import java.util.Locale;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/09/15 01:34.
 */
public class RakamUIConfig {
    private String ui;
    private String directory;
    private RakamUIModule.CustomPageBackend customPageBackend;
    private File customPageBackendDirectory;

    @Config("ui.source")
    public RakamUIConfig setUI(String ui) {
        this.ui = ui;
        return this;
    }

    public String getUI() {
        return ui;
    }

    @Config("ui.directory")
    public RakamUIConfig setDirectory(String directory) {
        this.directory = directory;
        return this;
    }

    public String getDirectory() {
        return directory;
    }

    @Config("ui.custom-page.backend")
    public RakamUIConfig setCustomPageBackend(String customPageBackend) {
        this.customPageBackend = RakamUIModule.CustomPageBackend.valueOf(customPageBackend.toUpperCase(Locale.CHINESE));
        return this;
    }

    public RakamUIModule.CustomPageBackend getCustomPageBackend() {
        return customPageBackend;
    }

    @Config("ui.custom-page.backend.directory")
    public RakamUIConfig setCustomPageBackendDirectory(File customPageBackendDirectory) {
        this.customPageBackendDirectory = customPageBackendDirectory;
        return this;
    }

    public File getCustomPageBackendDirectory() {
        return customPageBackendDirectory;
    }
}
