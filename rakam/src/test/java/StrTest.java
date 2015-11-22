import org.junit.Test;
import org.rakam.automation.action.ClientAutomationAction;

public class StrTest {
    @Test
    public void testName() throws Exception {
        ClientAutomationAction.StringTemplate stringTemplate = new ClientAutomationAction.StringTemplate("hey {name}!");
        String format = stringTemplate.format((name) -> "{namea}");
        System.out.println(format);
    }
}
