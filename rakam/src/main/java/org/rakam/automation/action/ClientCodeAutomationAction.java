package org.rakam.automation.action;

import org.rakam.automation.AutomationAction;
import org.rakam.plugin.user.User;

import java.util.function.Supplier;

public class ClientCodeAutomationAction
        implements AutomationAction<String> {

    public String process(String project, Supplier<User> user, String data) {
        return data;
    }
}
