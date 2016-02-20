package org.rakam.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import org.rakam.util.CryptUtil;

public class EncryptionConfig {
    private String secretKey = CryptUtil.generateRandomKey(70);

    public String getSecretKey() {
        return secretKey;
    }

    @Config("secret-key")
    @ConfigDescription("The secret key that will be used when encrypting sessions and passwords. " +
            "Do not expose this key because if it's known, the sessions may be hijacked. " +
            "If you don't set a secret key, it will be generated randomly for every restart.")
    public EncryptionConfig setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }
}
