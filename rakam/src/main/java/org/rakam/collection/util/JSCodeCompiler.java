package org.rakam.collection.util;

import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.apache.http.client.HttpClient;
import org.rakam.analysis.ConfigManager;
import org.rakam.plugin.CustomEventMapperHttpService;
import org.rakam.plugin.RAsyncHttpClient;
import org.rakam.util.CryptUtil;

import javax.inject.Inject;
import javax.inject.Named;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

public class JSCodeCompiler
{
    private final @Named("rakam-client") RAsyncHttpClient httpClient;
    private final ConfigManager configManager;

    @Inject
    public JSCodeCompiler(ConfigManager configManager, @Named("rakam-client") RAsyncHttpClient httpClient)
    {
        this.configManager = configManager;
        this.httpClient = httpClient;
    }

    public static class NashornEngineFilter
            implements ClassFilter
    {
        private final static String CRYPT_UTIL = CryptUtil.class.getName();

        @Override
        public boolean exposeToScripts(String s)
        {
            if (s.equals(CRYPT_UTIL)) {
                return true;
            }
            return false;
        }
    }

    public Invocable createEngine(String code)
            throws ScriptException
    {
        NashornScriptEngineFactory factory = new NashornScriptEngineFactory();

        ScriptEngine engine = factory
                .getScriptEngine(new String[] {"-strict", "--no-syntax-extensions"},
                        CustomEventMapperHttpService.class.getClassLoader(), new NashornEngineFilter());

        final Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.remove("print");
        bindings.remove("load");
        bindings.remove("loadWithNewGlobal");
        bindings.remove("exit");
//        bindings.remove("Java");
        bindings.remove("quit");
        bindings.put("config", configManager);
        bindings.put("http", httpClient);

        engine.eval(code);

        return (Invocable) engine;
    }
}
