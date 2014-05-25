package org.rakam.analysis.script.mvel;

import org.mvel2.MVEL;
import org.mvel2.ParserConfiguration;
import org.mvel2.ParserContext;
import org.rakam.analysis.script.FilterScript;
import org.rakam.util.UnboxedMathUtils;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Created by buremba on 04/05/14.
 */
public class MvelFilterScript extends FilterScript {
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
    private final Serializable script;
    private final boolean requiresUser;

    public MvelFilterScript(String script) {
        this.script = MVEL.compileExpression(script, new ParserContext(parserConfiguration));
        requiresUser = script.contains("_user.");
    }

    @Override
    public boolean test(JsonObject obj, JsonObject user) {
        if(requiresUser && user!=null)
            for(String key : user.getFieldNames())
                obj.putString("_user."+key, user.getString(key));
        Object ret = MVEL.executeExpression(script, obj);
        return ret!=null && !ret.equals(false);
    }

    @Override
    public boolean requiresUser() {
        return requiresUser;
    }

    @Override
    public String toString() {
        return script.toString().trim();
    }
}
