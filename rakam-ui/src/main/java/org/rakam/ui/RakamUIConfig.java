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
import io.airlift.units.Duration;
import org.rakam.ui.RakamUIModule.CustomPageBackend;

import java.io.File;
import java.net.URL;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class RakamUIConfig {
    private File uiDirectory;
    private CustomPageBackend customPageBackend;
    private File customPageBackendDirectory;
    private boolean enableUi;
    private boolean hashPassword;
    private String googleClientId;
    private String stripeKey;
    private URL screenCaptureService;
    private String authentication;
    private boolean disableTracking;
    private Duration cookieDuration = Duration.succinctDuration(30, TimeUnit.DAYS);
    private String intercomSecretKey;

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

    public String getAuthentication() {
        return authentication;
    }

    @Config("ui.authentication")
    public RakamUIConfig setAuthentication(String authentication) {
        this.authentication = authentication;
        return this;
    }

    public String getStripeKey() {
        return stripeKey;
    }

    @Config("stripe.key")
    public RakamUIConfig setStripeKey(String stripeKey) {
        this.stripeKey = stripeKey;
        return this;
    }

    public URL getScreenCaptureService() {
        return screenCaptureService;
    }

    @Config("ui.screen-capture-service-url")
    public RakamUIConfig setScreenCaptureService(URL screenCaptureService) {
        this.screenCaptureService = screenCaptureService;
        return this;
    }

    public File getUIDirectory() {
        return uiDirectory;
    }

    @Config("ui.directory")
    public RakamUIConfig setUIDirectory(File uiDirectory) {
        this.uiDirectory = uiDirectory;
        return this;
    }

    public CustomPageBackend getCustomPageBackend() {
        return customPageBackend;
    }

    @Config("ui.custom-page.backend")
    public RakamUIConfig setCustomPageBackend(String customPageBackend) {
        this.customPageBackend = CustomPageBackend.valueOf(customPageBackend.toUpperCase(Locale.CHINESE));
        return this;
    }

    public File getCustomPageBackendDirectory() {
        return customPageBackendDirectory;
    }

    @Config("ui.custom-page.backend.directory")
    public RakamUIConfig setCustomPageBackendDirectory(File customPageBackendDirectory) {
        this.customPageBackendDirectory = customPageBackendDirectory;
        return this;
    }

    public String getGoogleClientId() {
        return googleClientId;
    }

    @Config("ui.google-login-client-id")
    public RakamUIConfig setGoogleClientId(String googleClientId) {
        this.googleClientId = googleClientId;
        return this;
    }

    public String getIntercomSecretKey() {
        return intercomSecretKey;
    }

    @Config("ui.tracking.intercom.secret-key")
    public RakamUIConfig setIntercomSecretKey(String intercomSecretKey) {
        this.intercomSecretKey = intercomSecretKey;
        return this;
    }

    public boolean getDisableTracking() {
        return disableTracking;
    }

    @Config("ui.disable-tracking")
    public RakamUIConfig setDisableTracking(boolean disableTracking) {
        this.disableTracking = disableTracking;
        return this;
    }

    public Duration getCookieDuration() {
        return cookieDuration;
    }

    @Config("ui.cookie-duration")
    public RakamUIConfig setCookieDuration(Duration cookieDuration) {
        this.cookieDuration = cookieDuration;
        return this;
    }
}
