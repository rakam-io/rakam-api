package org.rakam.automation.action;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.rakam.automation.AutomationAction;
import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.User;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

@JsonTypeName("event")
public class SendEventAutomationAction implements AutomationAction<SendEventAutomationAction.SendEventAction> {

    private final Set<EventMapper> eventMappers;
    private final EventStore eventStore;

    @Inject
    public SendEventAutomationAction(Set<EventMapper> eventMappers, EventStore eventStore) {
        this.eventMappers = eventMappers;
        this.eventStore = eventStore;
    }

    public String process(Supplier<User> user, SendEventAction sendEventAction) {
        new Event(user.get().project, sendEventAction.collectionName, null, null);
        return null;
    }

    public static class SendEventAction {
        public final String collectionName;
        public final Map<String, Object> eventProperties;

        public SendEventAction(String eventName, Map<String, Object> eventProperties) {
            this.collectionName = eventName;
            this.eventProperties = eventProperties;
        }
    }
}
