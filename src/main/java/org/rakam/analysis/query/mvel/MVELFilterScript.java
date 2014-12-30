package org.rakam.analysis.query.mvel;

import org.mvel2.MVEL;
import org.mvel2.ParserConfiguration;
import org.mvel2.ParserContext;
import org.rakam.analysis.query.FilterScript;
import org.rakam.util.UnboxedMathUtils;
import org.rakam.util.json.JsonObject;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Created by buremba on 04/05/14.
 */
public class MVELFilterScript implements FilterScript {
    private final static ParserConfiguration parserConfiguration = new ParserConfiguration();

    static {
        parserConfiguration.addPackageImport("java.util");
        parserConfiguration.addImport("time", MVEL.getStaticMethod(System.class, "currentTimeMillis", new Class[0]));
        for (Method m : UnboxedMathUtils.class.getMethods()) {
            if ((m.getModifiers() & Modifier.STATIC) > 0) {
                parserConfiguration.addImport(m.getName(), m);
            }
        }
    }

    private final Serializable compiledScript;
    private final String script;
    private final boolean requiresUser;

    public MVELFilterScript(String script) {
        this.script = script;
        this.compiledScript = MVEL.compileExpression(script, new ParserContext(parserConfiguration));
        requiresUser = script.contains("_user.");
    }

    @Override
    public boolean test(JsonObject obj) {
        Object ret = MVEL.executeExpression(script, obj);
        return ret != null && !ret.equals(false);
    }

    @Override
    public boolean requiresUser() {
        return requiresUser;
    }

    @Override
    public org.rakam.util.json.JsonElement toJson() {
        return new JsonObject().putString("script", script);
    }
}
