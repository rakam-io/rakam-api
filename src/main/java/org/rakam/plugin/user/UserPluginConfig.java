package org.rakam.plugin.user;

import io.airlift.configuration.Config;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:27.
 */
public class UserPluginConfig {
    private Class storageClass;

    @Config("plugin.user.storage")
    public UserPluginConfig setStorageClass(String clazz)
    {
        try {
            this.storageClass = Class.forName(clazz);
        } catch (ClassNotFoundException e) {
           new ClassNotFoundException(format("plugin.user.storage value is invalid. '%s' couldn't found", clazz));
        }

        checkArgument(UserStorage.class.isAssignableFrom(this.storageClass),
                "plugin.user.storage value '%s' is not assignable to %s", clazz, UserStorage.class.getCanonicalName());

        return this;
    }

    public Class getStorageClass() {
        return storageClass;
    }
}
