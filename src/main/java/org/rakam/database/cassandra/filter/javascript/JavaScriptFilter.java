package org.rakam.database.cassandra.filter.javascript;

/**
 * Created by buremba on 21/12/13.
 */
import java.util.Map;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;
import org.rakam.database.cassandra.filter.Filter;

class JavaScriptFilter implements Filter {

    private Context context;

    private Scriptable scope;

    private Function function;

    public JavaScriptFilter(Context context, Scriptable scope, Function function) {
        this.context = context;
        this.scope = scope;
        this.function = function;
    }

    @Override
    public Map filter(Map row) {
        // Unfortunately Rhino does not treat a Map passed into JavaScript as a native JS
        // object which really sucks; so, until I find a better solution, I am converting
        // the map passed as an argument into a native JS object. Rhino does however have
        // its native objects now implement Map so we do not have to do a mapping on the
        // object returned from the function call.
        Scriptable jsObject = context.newObject(scope);
        for (Object key : row.keySet()) {
            jsObject.put(key.toString(), jsObject, row.get(key));
        }
        return (Map) function.call(context, scope, scope, new Object[] {jsObject});
    }
}