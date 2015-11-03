package org.rakam.automation;

import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public class AutomationEventMapper implements EventMapper {

    private static final String PROPERTY_KEY = "_auto";
    private final UserAutomationService service;

    public AutomationEventMapper(UserAutomationService service) {
        this.service = service;
    }

    @Override
    public Iterable<Map.Entry<String, String>> map(Event event, Iterable<Map.Entry<String, String>> extraProperties, InetAddress sourceAddress) {
        final List<UserAutomationService.AutomationRule> automationRules = service.get(event.project());
        if(automationRules != null) {
            String value = null;
            for (Map.Entry<String, String> extraProperty : extraProperties) {
                if(PROPERTY_KEY.equals(extraProperty.getKey())) {
                    value = extraProperty.getValue();
                    break;
                }
            }

            if(value == null) {

            }

            for (UserAutomationService.AutomationRule automationRule : automationRules) {
                if(automationRule.eventFilter != null) {
//                    if(!event.collection().equals(automationRule.eventFilter.collection) || automationRule.eventFilter.filterExpression) {
//                        continue;
//                    }
                }
            }
        }
        return null;
    }
}
