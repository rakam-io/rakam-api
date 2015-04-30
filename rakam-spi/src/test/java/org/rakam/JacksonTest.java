package org.rakam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;
import org.rakam.util.JsonHelper;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/04/15 02:45.
 */
public class JacksonTest {

    @Test
    public void parseTest() {
        String str = "{\"SortAs\": \"SGML\", \"GlossTerm\": \"Standard Generalized Markup Language\", \"Acronym\": \"SGML\", \"ali\": 4}";

        for (int i = 0; i < 30000; i++) {
            JsonNode read = JsonHelper.read(str);
        }

        long l = System.currentTimeMillis();
        for (int i = 0; i < 5000000; i++) {
            JsonNode read = JsonHelper.read(str);
        }
        System.out.println(System.currentTimeMillis() - l);
    }

    @Test
    public void parseTest2() {
        String str = "{\"SortAs\": \"SGML\", \"GlossTerm\": \"Standard Generalized Markup Language\", \"Acronym\": \"SGML\", \"ali\": 4}";

        for (int i = 0; i < 30000; i++) {
            Bean read = JsonHelper.read(str, Bean.class);
        }

        long l = System.currentTimeMillis();
        for (int i = 0; i < 5000000; i++) {
            Bean read = JsonHelper.read(str, Bean.class);
        }
        System.out.println(System.currentTimeMillis() - l);
    }

    public static class Bean {
        public final String SortAs;
        public final String GlossTerm;
        public final String Acronym;
        public final long ali;

        @JsonCreator
        public Bean(@JsonProperty("sortAs") String sortAs,
                    @JsonProperty("glossTerm") String glossTerm,
                    @JsonProperty("acronym") String acronym,
                    @JsonProperty("ali") long ali) {
            SortAs = sortAs;
            GlossTerm = glossTerm;
            Acronym = acronym;
            this.ali = ali;
        }
    }
}