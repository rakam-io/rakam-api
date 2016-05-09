package org.rakam.automation;

import org.rakam.plugin.user.User;

import java.util.function.Supplier;

public interface AutomationAction<T> {
    String process(String project, Supplier<User> user, T actionData);
}
