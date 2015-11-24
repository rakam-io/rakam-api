package org.rakam.automation.action;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.automation.AutomationAction;
import org.rakam.plugin.user.User;
import org.rakam.util.StringTemplate;

import java.util.Map;
import java.util.function.Supplier;

public class ClientAutomationAction implements AutomationAction<ClientAutomationAction.Template> {

    public String process(Supplier<User> user, Template data) {
        StringTemplate template = new StringTemplate(data.template);
        return template.format((query) -> {
            Object val = user.get().properties.get(query);
            if(val == null || !(val instanceof String)) {
                return data.variables.get(query);
            }
            return val.toString();
        });
    }

    public static class Template {
        public final String template;
        public final Map<String, String> variables;

        @JsonCreator
        public Template(@JsonProperty("template") String template, @JsonProperty("variables") Map<String, String> variables) {
            this.template = template;
            this.variables = variables;
        }
    }
}
