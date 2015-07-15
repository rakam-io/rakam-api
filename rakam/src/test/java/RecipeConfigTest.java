import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import org.rakam.Recipe;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 13/07/15 12:39.
 */
public class RecipeConfigTest {
    @Test
    public void test() throws IOException {
        InputStream io = getClass().getResource("ecommerce_test.yml").openStream();
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Recipe recipe = mapper.readValue(io, Recipe.class);

    }
}
