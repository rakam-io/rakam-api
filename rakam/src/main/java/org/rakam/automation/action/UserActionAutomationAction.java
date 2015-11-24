package org.rakam.automation.action;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.rakam.automation.AutomationAction;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserActionService;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@JsonTypeName("user_action")
public class UserActionAutomationAction implements AutomationAction<UserActionAutomationAction.UserAction> {
    private final Map<String, UserActionService> userActionServiceMap;

    @Inject
    public UserActionAutomationAction(Set<UserActionService> userActionServices) {
        userActionServiceMap = userActionServices.stream()
                .collect(Collectors.toMap(UserActionService::getName, a -> a));
    }

    public String process(Supplier<User> user, UserAction actionData) {
        userActionServiceMap.get(actionData.actionName).send(user.get(), actionData.actionData);
        return null;
    }

    public static class UserAction {
        public final String actionName;
        public final Object actionData;

        public UserAction(String actionName, Object actionData) {
            this.actionName = actionName;
            this.actionData = actionData;
        }
    }
}
