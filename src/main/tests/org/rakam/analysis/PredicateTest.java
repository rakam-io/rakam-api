package org.rakam.analysis;

import org.junit.Test;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.rakam.analysis.AnalysisRuleParser.generatePredicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/09/14 15:38.
 */
public class PredicateTest {
    @Test
    public void predicateTest() {
        JsonObject json = new JsonObject("{\n" +
                "    \"AND\": [\n" +
                "      [\"field1\", \"$gte\", 20],\n" +
                "      [\"field\", \"$in\", [20, 30, 40]]\n" +
                "    ]\n" +
                "  }");
        generatePredicate(json).equals(generatePredicate(json));
    }

    @Test
    public void ruleTest() {
        JsonObject json = new JsonObject("{\n" +
                "  \"strategy\": \"REAL_TIME\",\n" +
                "  \"_tracking\": \"e74607921dad4803b998\",\n" +
                "  \"analysis\": \"TIMESERIES\",\n" +
                "  \"aggregation\": \"COUNT\",\n" +
                "  \"interval\": \"1hour\",\n" +
                "  \"filter\": {\n" +
                "    \"AND\": [\n" +
                "      [\"field1\", \"$gte\", 20],\n" +
                "      [\"field\", \"$in\", [20, 30, 40]]\n" +
                "    ]\n" +
                "  }\n" +
                "}");
        AnalysisRule parse0 = AnalysisRuleParser.parse(json);
        AnalysisRule parse1 = AnalysisRuleParser.parse(json);
        assertTrue(parse0.equals(parse1));
        assertEquals(parse0.hashCode(), parse1.hashCode());
    }
}
