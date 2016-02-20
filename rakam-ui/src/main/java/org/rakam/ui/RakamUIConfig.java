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
import io.airlift.configuration.ConfigDescription;
import org.rakam.ui.RakamUIModule.CustomPageBackend;

import java.io.File;
import java.util.Locale;

public class RakamUIConfig {
    private File uiDirectory;
    private CustomPageBackend customPageBackend = CustomPageBackend.JDBC;
    private File customPageBackendDirectory;
    private boolean enableUi = true;
    private boolean hashPassword = false;
    private String secretKey;

    @Config("ui.directory")
    public RakamUIConfig setUIDirectory(File uiDirectory) {
        this.uiDirectory = uiDirectory;
        return this;
    }

    public boolean getHashPassword() {
        return hashPassword;
    }

    // TODO: should we use sha256 instead of sha1 for hashing password in order to be able to sure that we won't get any collision?
    @Config("ui.hash-password")
    @ConfigDescription("Set true if you want passwords to be hashed with ui.secret-key before encrypting with one-way hashing algorithm. " +
            "If you modify this key, all passwords saved in database will be invalidated.")
    public RakamUIConfig setHashPassword(boolean hashPassword) {
        this.hashPassword = hashPassword;
        return this;
    }

    public boolean getEnableUI() {
        return enableUi;
    }

    @Config("ui.enable")
    public RakamUIConfig setEnableUI(boolean enableUi) {
        this.enableUi = enableUi;
        return this;
    }

    public File getUIDirectory() {
        return uiDirectory;
    }

    @Config("ui.custom-page.backend")
    public RakamUIConfig setCustomPageBackend(String customPageBackend) {
        this.customPageBackend = CustomPageBackend.valueOf(customPageBackend.toUpperCase(Locale.CHINESE));
        return this;
    }

    public CustomPageBackend getCustomPageBackend() {
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

    public String getSecretKey() {
        return secretKey;
    }

    @Config("secret-key")
    @ConfigDescription("The secret key that will be used when encrypting sessions and passwords. " +
            "Do not expose this key because if it's known, the sessions may be hijacked.")
    public RakamUIConfig setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }
}
