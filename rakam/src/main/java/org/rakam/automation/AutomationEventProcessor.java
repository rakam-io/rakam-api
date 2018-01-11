package org.rakam.automation;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import org.rakam.Mapper;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.Event;
import org.rakam.config.EncryptionConfig;
import org.rakam.plugin.SyncEventMapper;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserStorage;
import org.rakam.util.CryptUtil;

import javax.inject.Inject;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

@Singleton
@Mapper(name = "Automation Event Processor", description = "Processes automation rules and take action if the user is completed the steps")
public class AutomationEventProcessor implements SyncEventMapper {
    private static final String PROPERTY_KEY = "_auto";
    private static final String PROPERTY_ACTION_KEY = "_auto_action";
    private static final List<Cookie> clearData;

    static {
        DefaultCookie defaultCookie = new DefaultCookie(PROPERTY_KEY, "");
        defaultCookie.setMaxAge(0);
        clearData = ImmutableList.of(defaultCookie);
    }

    private final Provider<UserStorage> userStorageProvider;
    private final Provider<UserAutomationService> serviceProvider;
    private final EncryptionConfig encryptionConfig;
    private UserAutomationService service;
    private UserStorage userStorage;

    @Inject
    public AutomationEventProcessor(
            Provider<UserAutomationService> service,
            Provider<UserStorage> storage,
            EncryptionConfig encryptionConfig) {
        this.encryptionConfig = encryptionConfig;
        this.userStorageProvider = storage;
        this.serviceProvider = service;
    }

    @Override
    public void init() {
        this.userStorage = userStorageProvider.get();
        this.service = serviceProvider.get();
    }

    @Override
    public List<Cookie> map(Event event, RequestParams extraProperties, InetAddress sourceAddress, HttpHeaders responseHeaders) {
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
            if (value != null) {
                for (ScenarioState scenarioState : value) {
                    if (ruleId == scenarioState.ruleId) {
                        state = scenarioState;
                    }
                }
            }

            if (state == null) {
                if (newStates == null) {
                    newStates = new ScenarioState[automationRules.size()];

                    if (value != null) {
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

                if (state.state >= automationRule.scenarios.size()) {
                    state.state = 0;
                    state.threshold = 0;
                    // state is already changed
                    if (actions == null) {
                        actions = new ArrayList<>();
                    }

                    for (AutomationRule.SerializableAction action : automationRule.actions) {
                        Supplier<User> supplier = new Supplier<User>() {
                            private User user;

                            @Override
                            public User get() {
                                if (user == null) {
                                    String userAttr = event.getAttribute("_user");
                                    if (userAttr != null) {
                                        user = userStorage.getUser(new RequestContext(event.project(), null), userAttr).join();
                                    }
                                }
                                return user;
                            }
                        };

                        action.getAction().process(event.project(), supplier, action.value);
                    }
                }
            }
        }

        if (actions != null) {
            StringBuilder builder = new StringBuilder();
            Base64.Encoder encoder = Base64.getEncoder();

            for (String action : actions) {
                if (builder.length() != 0) {
                    builder.append(',');
                }
                try {
                    builder.append(encoder.encodeToString(action.getBytes("UTF-8")));
                } catch (UnsupportedEncodingException e) {
                    throw Throwables.propagate(e);
                }

            }

            responseHeaders.set(PROPERTY_ACTION_KEY, builder.toString());
        }

        return stateChanged ? ImmutableList.of(new DefaultCookie(PROPERTY_KEY, encodeState(newStates == null ? value : newStates))) : null;
    }

    private String encodeState(ScenarioState[] states) {
        if (states == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (ScenarioState scenarioState : states) {
            if (scenarioState != null) {
                builder.append(scenarioState.ruleId).append(':').append(scenarioState.state).append(':').append(scenarioState.threshold);
            }
        }

        String secureKey = CryptUtil.encryptWithHMacSHA1(builder.toString(), encryptionConfig.getSecretKey());
        builder.append('|').append(secureKey);

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

    private ScenarioState[] extractState(RequestParams extraProperties) throws IllegalStateException {
        ScenarioState[] value;

        String val = extraProperties.cookies().stream().filter(e -> e.name().equals(PROPERTY_KEY))
                .findAny().map(e -> e.value()).orElse(null);
        if (val == null) {
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
