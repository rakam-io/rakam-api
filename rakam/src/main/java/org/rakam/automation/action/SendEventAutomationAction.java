package org.rakam.automation.action;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.automation.AutomationAction;
import org.rakam.collection.Event;
import org.rakam.plugin.user.User;

import java.util.Map;
import java.util.function.Supplier;

public class SendEventAutomationAction implements AutomationAction<SendEventAutomationAction.SendEventAction> {


    public String process(String project, Supplier<User> user, SendEventAction sendEventAction) {
        new Event(project, sendEventAction.collection, null, null, null);
        return null;
    }

    public static class SendEventAction {
        public final String collection;
        public final Map<String, Object> properties;

        @JsonCreator
        public SendEventAction(@JsonProperty("collection") String collection,
                               @JsonProperty("properties") Map<String, Object> properties) {
            this.collection = collection;
            this.properties = properties;
        }
    }
}
