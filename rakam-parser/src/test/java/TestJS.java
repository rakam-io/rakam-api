import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class TestJS
{

    class MyCF implements ClassFilter
    {
        @Override
        public boolean exposeToScripts(String s) {
            if (s.compareTo("java.io.File") == 0) return false;
            return true;
        }
    }
    
    public void testName()
            throws Exception
    {

        ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine(new MyCF());
        engine.eval("quit(); var MyJavaClass = Java.type('java.lang.System'); MyJavaClass.exit(0); var module = function(a) { System.exit(0); return a}");
        Invocable invocable = (Invocable) engine;

        Object result = invocable.invokeFunction("module", "Peter Parker");
        System.out.println(result);
        System.out.println(result.getClass());
    }
}
