package org.rakam.automation;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import org.rakam.collection.Event;
import org.rakam.config.EncryptionConfig;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserStorage;
import org.rakam.util.CryptUtil;

import javax.inject.Inject;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class AutomationEventProcessor implements EventProcessor {

    private static final String PROPERTY_KEY = "_auto";
    private static final String PROPERTY_ACTION_KEY = "_auto_action";

    private final UserAutomationService service;
    private final UserStorage userStorage;
    private final EncryptionConfig encryptionConfig;

    private static final List<Cookie> clearData;

    static {
        DefaultCookie defaultCookie = new DefaultCookie(PROPERTY_KEY, "");
        defaultCookie.setMaxAge(0);
        clearData = ImmutableList.of(defaultCookie);
    }


    @Inject
    public AutomationEventProcessor(UserAutomationService service, UserStorage userStorage, EncryptionConfig encryptionConfig) {
        this.service = service;
        this.userStorage = userStorage;
        this.encryptionConfig = encryptionConfig;
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        final List<AutomationRule> automationRules = service.list(event.project());
        if (automationRules == null) {
            return null;
        }

        ScenarioState[] value;
        try {
            value = extractState(extraProperties);
        } catch (IllegalStateException e) {
            return clearData;
        }

        boolean stateChanged = false;
        List<String> actions = null;

        ScenarioState[] newStates = null;
        int newIdx = 0;
        for (AutomationRule automationRule : automationRules) {
            if (!automationRule.isActive) {
                continue;
            }
            int ruleId = automationRule.id;
            ScenarioState state = null;
            if(value != null) {
                for (ScenarioState scenarioState : value) {
                    if (ruleId == scenarioState.ruleId) {
                        state = scenarioState;
                    }
                }
            }

            if(state == null) {
                if(newStates == null) {
                    newStates = new ScenarioState[automationRules.size()];

                    if(value != null) {
                        for (ScenarioState scenarioState : value) {
                            newStates[newIdx++] = scenarioState;
                        }
                    }
                }
                state = new ScenarioState(ruleId, 0, 0);
                newStates[newIdx++] = state;
            }

            AutomationRule.ScenarioStep scenarioStep = automationRule.scenarios.get(state.state);
            if (event.collection().equals(scenarioStep.collection) && scenarioStep.filterPredicate.test(event)) {

                stateChanged |= updateState(scenarioStep, state, event);

                if(state.state >= automationRule.scenarios.size()) {
                    state.state = 0;
                    state.threshold = 0;
                    // state is already changed
                    if(actions == null) {
                        actions = new ArrayList<>();
                    }

                    for (AutomationRule.SerializableAction action : automationRule.actions) {
                        Supplier<User> supplier = new Supplier<User>() {
                            User user;

                            @Override
                            public User get() {
                                if (user == null) {
                                    String userAttr = event.getAttribute("_user");
                                    if (userAttr != null) {
                                        user = userStorage.getUser(event.project(), userAttr).join();
                                    }
                                }
                                return user;
                            }
                        };

                        action.getAction().process(supplier, action.value);
                    }
                }
            }
        }

        if(actions != null) {
            StringBuilder builder = new StringBuilder();
            Base64.Encoder encoder = Base64.getEncoder();

            for (String action : actions) {
                if(builder.length() != 0) {
                    builder.append(',');
                }
                try {
                    builder.append(encoder.encodeToString(action.getBytes("UTF-8")));
                } catch (UnsupportedEncodingException e) {
                    throw Throwables.propagate(e);
                }

            }

            response.headers().set(PROPERTY_ACTION_KEY, builder.toString());
        }

        return  stateChanged ? ImmutableList.of(new DefaultCookie(PROPERTY_KEY, encodeState(newStates == null ? value : newStates))) : null;
    }

    private String encodeState(ScenarioState[] states) {
        if(states == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (ScenarioState scenarioState : states) {
            if(scenarioState != null) {
                builder.append(scenarioState.ruleId).append(":").append(scenarioState.state).append(":").append(scenarioState.threshold);
            }
        }

        String secureKey = CryptUtil.encryptWithHMacSHA1(builder.toString(), encryptionConfig.getSecretKey());
        builder.append("|").append(secureKey);

        return builder.toString();
    }

    private boolean updateState(AutomationRule.ScenarioStep scenarioStep, ScenarioState state, Event event) {
        switch (scenarioStep.threshold.aggregation) {
            case count:
                String fieldName = scenarioStep.threshold.fieldName;
                if (fieldName == null || (fieldName != null && event.getAttribute(fieldName) != null)) {
                    if (state.threshold == scenarioStep.threshold.value) {
                        state.state += 1;
                        state.threshold = 0;
                    } else {
                        state.threshold += 1;
                    }
                    return true;
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
                    return true;
                }
                break;
        }
        return false;
    }

    private ScenarioState[] extractState(Iterable<Map.Entry<String, String>> extraProperties) throws IllegalStateException {
        ScenarioState[] value;
        for (Map.Entry<String, String> extraProperty : extraProperties) {
            if ("Cookie".equals(extraProperty.getKey())) {
                String val = null;
                Set<Cookie> decode = ServerCookieDecoder.STRICT.decode(extraProperty.getValue());
                for (Cookie cookie : decode) {
                    if(cookie.name().equals(PROPERTY_KEY)) {
                        val = cookie.value();
                    }
                }
                if(val == null) {
                    return null;
                }
                String[] cookie = val.split("\\|", 2);

                if (cookie.length != 2 || !CryptUtil.encryptWithHMacSHA1(cookie[0], encryptionConfig.getSecretKey()).equals(cookie[1])) {
                    throw new IllegalStateException();
                }

                String[] values = cookie[0].split(",");
                value = new ScenarioState[values.length];
                for (int i = 0; i < values.length; i++) {
                    final String[] split = values[i].split(":", 3);
                    final ScenarioState scenarioState;
                    try {
                        scenarioState = new ScenarioState(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2]));
                    } catch (NumberFormatException e) {
                        throw new IllegalStateException();
                    }
                    value[i] = scenarioState;
                }
                break;
            }
        }
        return null;
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
}
