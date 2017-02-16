import okhttp3.OkHttpClient;
import org.rakam.TestingConfigManager;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.plugin.RAsyncHttpClient;
import org.testng.annotations.Test;

import javax.script.ScriptException;

public class TestJSCodeCompiler
{
    @Test
    public void testName()
            throws ScriptException
    {

        JSCodeCompiler jsCodeCompiler = new JSCodeCompiler(new TestingConfigManager(),
                new RAsyncHttpClient(new OkHttpClient()),
                (project, prefix) -> new JSCodeCompiler.TestLogger(), false);
//        jsCodeCompiler.createEngine("test", "new Array(100000000).concat(new Array(100000000));", "");
    }
}
