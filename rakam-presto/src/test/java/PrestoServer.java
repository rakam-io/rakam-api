import com.facebook.presto.server.testing.TestingPrestoServer;
import org.testng.annotations.Test;

public class PrestoServer {
    @Test
    public void testName() throws Exception {
        TestingPrestoServer testingPrestoServer = new TestingPrestoServer();
        System.out.println(testingPrestoServer.getAddress());

    }
}
