package org.rakam.automation;

import org.rakam.automation.action.ClientCodeAutomationAction;
import org.rakam.automation.action.ClientMessageAutomationAction;
import org.rakam.automation.action.SendEventAutomationAction;
import org.rakam.automation.action.UserActionAutomationAction;

public enum AutomationActionType {
    client_message(ClientMessageAutomationAction.class),
    client_code(ClientCodeAutomationAction.class),
    user_action(UserActionAutomationAction.class),
    event(SendEventAutomationAction.class);

    private final Class<? extends AutomationAction> actionClazz;

    AutomationActionType(Class<? extends AutomationAction> actionClazz) {
        this.actionClazz = actionClazz;
    }

    public Class<? extends AutomationAction> getActionClass() {
        return actionClazz;
    }
}
