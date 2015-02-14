package org.rakam.analysis;

import org.junit.Test;
import org.rakam.analysis.rule.aggregation.AggregationReport;
import org.rakam.util.json.JsonObject;

import static junit.framework.Assert.assertEquals;
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
}
