package org.rakam.automation.action;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.rakam.automation.AutomationAction;
import org.rakam.plugin.user.User;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("client")
public class ClientAutomationAction implements AutomationAction<ClientAutomationAction.StringTemplate> {

    public String process(Supplier<User> user, StringTemplate data) {
        return data.format((query) -> {
            if(query.startsWith("_user.")) {
                Object val = user.get().properties.get(query.substring(6));
                return val == null ? null : val.toString();
            } else {
                return null;
            }
        });
    }

    public static class StringTemplate {
        private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{([^/?}]+)\\}");

        private final Matcher matcher;
        private final boolean requiresUser;
        private final String template;

        @JsonCreator
        public StringTemplate(@JsonProperty("template") String template) {
            matcher = VARIABLE_PATTERN.matcher(template);
            this.template = template;
            while (matcher.find()) {
                if(matcher.group(1).startsWith("user.")) {
                    this.requiresUser = true;
                    return;
                }
            }
            this.requiresUser = false;
        }

        public String getTemplate() {
            return template;
        }

        public String format(Map<String, String> parameters) {
            return format(parameters::get);
        }

        public boolean requiresUser() {
            return requiresUser;
        }

        public String format(Function<String, String> replacement) {
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                matcher.appendReplacement(sb, replacement.apply(matcher.group(1)));
            }
            matcher.appendTail(sb);
            return sb.toString();
        }
    }
}
