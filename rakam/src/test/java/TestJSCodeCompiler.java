import okhttp3.OkHttpClient;
import org.rakam.TestingConfigManager;
import org.rakam.util.RAsyncHttpClient;
import org.rakam.util.javascript.JSCodeCompiler;
import org.testng.annotations.Test;

import javax.script.ScriptException;

public class TestJSCodeCompiler {
    @Test
    public void testName()
            throws ScriptException {

        JSCodeCompiler jsCodeCompiler = new JSCodeCompiler(new TestingConfigManager(),
                new RAsyncHttpClient(new OkHttpClient()),
                (project, prefix) -> new JSCodeCompiler.TestLogger(), false, true);
//        jsCodeCompiler.createEngine("test", "new Array(100000000).concat(new Array(100000000));", "");
    }
}
