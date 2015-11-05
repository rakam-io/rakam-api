package org.rakam.automation;

import com.google.common.collect.ImmutableList;
import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;

import java.net.InetAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AutomationEventMapper implements EventMapper {

    private static final String PROPERTY_KEY = "_auto";
    private static final String PROPERTY_ACTION_KEY = "_auto_action";
    private final UserAutomationService service;
    private final List clearData = ImmutableList.of(new AbstractMap.SimpleImmutableEntry<>(PROPERTY_KEY, null));

    public AutomationEventMapper(UserAutomationService service) {
        this.service = service;
    }

    private static class ScenarioState {
        public final int ruleId;
        public int state;
        public int threshold;

        public ScenarioState(int ruleId, int state, int threshold) {
            this.ruleId = ruleId;
            this.state = state;
            this.threshold = threshold;
        }
    }

    @Override
    public Iterable<Map.Entry<String, String>> map(Event event, Iterable<Map.Entry<String, String>> extraProperties, InetAddress sourceAddress) {
        final List<UserAutomationService.AutomationRule> automationRules = service.get(event.project());
        if (automationRules == null) {
            return null;
        }

        ScenarioState[] value = null;
        for (Map.Entry<String, String> extraProperty : extraProperties) {
            if (PROPERTY_KEY.equals(extraProperty.getKey())) {
                String[] cookie = extraProperty.getValue().split("\\|", 2);

                if (cookie.length != 2 || CryptUtil.encryptWithHMacSHA1(cookie[0], "secureKey").equals(cookie[1])) {
                    return clearData;
                }

                String[] values = cookie[0].split(",");
                value = new ScenarioState[values.length];
                for (int i = 0; i < values.length; i++) {
                    final String[] split = values[i].split(":", 3);
                    final ScenarioState scenarioState;
                    try {
                        scenarioState = new ScenarioState(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2]));
                    } catch (NumberFormatException e) {
                        return clearData;
                    }
                    value[i] = scenarioState;
                }
                break;
            }
        }

        boolean stateChanged = false;
        List<String> actions = null;

        for (UserAutomationService.AutomationRule automationRule : automationRules) {
            final List<UserAutomationService.ScenarioStep> scenarioSteps = automationRule.scenarioSteps;

            int ruleId = automationRule.id;
            ScenarioState state = null;
            for (ScenarioState scenarioState : value) {
                if (ruleId == scenarioState.ruleId) {
                    state = scenarioState;
                }
            }

            if (state == null) {
                continue;
            }

            UserAutomationService.ScenarioStep scenarioStep = scenarioSteps.get(state.state);
            if (event.collection().equals(scenarioStep.collection) && scenarioStep.filterPredicate.test(event)) {
                int newStateThreshold = state.threshold + 1;
                switch (scenarioStep.threshold.aggregation) {
                    case count:
                        String fieldName = scenarioStep.threshold.fieldName;
                        if (fieldName == null ||
                                (fieldName != null && event.getAttribute(fieldName) != null)) {
                            if (newStateThreshold == scenarioStep.threshold.value) {
                                state.state += 1;
                                state.threshold = 0;
                            } else {
                                state.threshold += 1;
                            }
                            stateChanged = true;
                        }
                        break;
                    case sum:
                        Number val = event.getAttribute(scenarioStep.threshold.fieldName);
                        if (val != null) {
                            int newVal = state.threshold + val.intValue();
                            if (state.threshold <= newVal) {
                                state.state += 1;
                                state.threshold = 0;
                            } else {
                                state.threshold += newVal;
                            }
                            stateChanged = true;
                        }
                        break;
                }

                if(state.state >= scenarioSteps.size()) {
                    state.state = 0;
                    state.threshold = 0;
                    // state is already changed
                    if(actions == null) {
                        actions = new ArrayList<>();
                    }
                    for (UserAutomationService.Action action : automationRule.actions) {
                        switch (action.type) {
                            case client:
                                actions.add(action.value);
                            case email:
                                // push action to a query to send emails on worker threads
                                break;
                        }
                    }
                }
            }
        }

        if (stateChanged) {
            StringBuilder builder = new StringBuilder();
            for (ScenarioState scenarioState : value) {
                builder.append(scenarioState.ruleId).append(":").append(scenarioState.state).append(":").append(scenarioState.threshold);
            }
            String secureKey = CryptUtil.encryptWithHMacSHA1(builder.toString(), "secureKey");
            builder.append(secureKey);

            AbstractMap.SimpleImmutableEntry<String, String> element = new AbstractMap.SimpleImmutableEntry<>(PROPERTY_KEY, builder.toString());
            if(actions == null) {
                return ImmutableList.of(element);
            } else {
                return ImmutableList.of(element, new AbstractMap.SimpleImmutableEntry<>(PROPERTY_ACTION_KEY, JsonHelper.encode(actions)));
            }
        }

        return null;
    }
}
