package org.rakam.automation.action;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.rakam.automation.AutomationAction;
import org.rakam.plugin.user.User;
import org.rakam.util.StringTemplate;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class ClientMessageAutomationAction
        implements AutomationAction<ClientMessageAutomationAction.Template> {

    public String process(String project, Supplier<User> user, Template data) {
        StringTemplate template = new StringTemplate(data.template);
        return template.format((query) -> {
            Object val = user.get().properties.get(query);
            if (val == null || !(val instanceof String)) {
                return data.variables.get(query);
            }
            return val.toString();
        });
    }

    public static class Template {
        public final String template;
        public final Map<String, String> variables;

        @JsonCreator
        public Template(@JsonProperty("template") String template,
                        @JsonProperty(value = "variables", required = false) Map<String, String> variables) {
            this.template = template;
            this.variables = Optional.ofNullable(variables).orElse(ImmutableMap.of());
        }
    }
}
