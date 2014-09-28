package org.rakam.analysis.query.mvel;

import org.mvel2.MVEL;
import org.mvel2.ParserConfiguration;
import org.mvel2.ParserContext;
import org.rakam.analysis.query.FieldScript;
import org.rakam.util.UnboxedMathUtils;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Created by buremba on 04/05/14.
 */
public class MVELFieldScript implements FieldScript {
    private final transient boolean userData;
    private final transient Serializable compiledScript;
    private final String script;

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

    public MVELFieldScript(String script) {
        this.script = script;
        this.compiledScript = MVEL.compileExpression(script, new ParserContext(parserConfiguration));
        userData = script.contains("_user.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MVELFieldScript)) return false;

        MVELFieldScript that = (MVELFieldScript) o;

        if (!script.equals(that.script)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    @Override
    public boolean requiresUser() {
        return userData;
    }

    @Override
    public JsonObject toJson() {
        return new JsonObject().putString("script", script);
    }

    @Override
    public Object extract(JsonObject event, JsonObject user) {
        if(userData) {
            event.mergeIn(user);
        }
        return MVEL.executeExpression(compiledScript, event);
    }

    @Override
    public boolean contains(JsonObject event, JsonObject user) {
        return extract(event,user)!=null;
    }
}
