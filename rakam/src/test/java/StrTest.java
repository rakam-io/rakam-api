import org.junit.Test;
import org.rakam.automation.action.ClientAutomationAction;

public class StrTest {
    @Test
    public void testName() throws Exception {
        ClientAutomationAction.StringTemplate stringTemplate = new ClientAutomationAction.StringTemplate("fsdf {test}ff");
        System.out.println(stringTemplate.format((name) -> null));
        System.out.println(stringTemplate.format((name) -> "{namea}"));
        System.out.println(stringTemplate.format((name) -> "{namea}"));
        System.out.println(stringTemplate.format((name) -> "{namea}"));
    }
}
