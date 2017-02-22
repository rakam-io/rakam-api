package org.rakam.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringTemplate {
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("(?<!\\\\)\\{([^/?}]+)\\}");

    private final String template;

    public StringTemplate(String template) {
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
