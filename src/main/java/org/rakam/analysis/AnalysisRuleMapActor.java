package org.rakam.analysis;

import org.rakam.util.json.JsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 26/10/14 16:04.
 */
public class AnalysisRuleMapActor {
    public final static int ADD = 0;
    public final static int DELETE = 1;
    public final static int UPDATE_BATCH = 2;

    private final AnalysisRuleMap map;

    public AnalysisRuleMapActor(AnalysisRuleMap map) {
        this.map = map;
    }


    public void handle(JsonObject json) {
        String project = json.getString("tracker");

        switch (json.getInteger("operation")) {
            case ADD:
//                map.add(project, AnalysisRuleParser.parse(json.getJsonObject("rule")));
                break;
            case DELETE:
//                map.remove(project, AnalysisRuleParser.parse(json.getJsonObject("rule")));
            case UPDATE_BATCH:
//                map.updateBatch(project, AnalysisRuleParser.parse(json.getJsonObject("rule")));
            default:
                throw new IllegalArgumentException("operation doesn't exist");
        }
    }
}
