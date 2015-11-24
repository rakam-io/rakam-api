import org.junit.Test;
import org.rakam.util.StringTemplate;

public class StrTest {
    @Test
    public void testName() throws Exception {
        StringTemplate stringTemplate = new StringTemplate("fsdf {test}ff");
        System.out.println(stringTemplate.format((name) -> null));
        System.out.println(stringTemplate.format((name) -> "{namea}"));
        System.out.println(stringTemplate.format((name) -> "{namea}"));
        System.out.println(stringTemplate.format((name) -> "{namea}"));
    }
}
