package org.rakam.automation.action;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.rakam.automation.AutomationAction;
import org.rakam.plugin.user.User;

import java.util.ArrayList;
import java.util.List;
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

        private final String template;

        @JsonCreator
        public StringTemplate(@JsonProperty("template") String template) {
            this.template = template;
        }

        public String getTemplate() {
            return template;
        }

        public String format(Map<String, String> parameters) {
            return format(parameters::get);
        }

        public List<String> getVariables() {
            List<String> vars = new ArrayList<>();
            Matcher matcher = VARIABLE_PATTERN.matcher(template);

            while (matcher.find()) {
                vars.add(matcher.group(1));
            }
            return vars;
        }

        public String format(Function<String, String> replacement) {
            StringBuffer sb = new StringBuffer();
            Matcher matcher = VARIABLE_PATTERN.matcher(template);

            while (matcher.find()) {
                String apply = replacement.apply(matcher.group(1));
                matcher.appendReplacement(sb, apply == null ? "" : apply);
            }
            matcher.appendTail(sb);
            return sb.toString();
        }
    }
}
