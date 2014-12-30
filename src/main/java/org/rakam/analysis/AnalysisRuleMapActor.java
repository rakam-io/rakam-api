package org.rakam.analysis;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.contrib.pattern.DistributedPubSubExtension;
import akka.contrib.pattern.DistributedPubSubMediator;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import org.rakam.util.json.JsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 26/10/14 16:04.
 */
public class AnalysisRuleMapActor extends UntypedActor {
    public final static int ADD = 0;
    public final static int DELETE = 1;
    public final static int UPDATE_BATCH = 2;

    private final AnalysisRuleMap map;

    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public AnalysisRuleMapActor(AnalysisRuleMap map) {
        ActorRef mediator = DistributedPubSubExtension.get(getContext().system()).mediator();
        mediator.tell(new DistributedPubSubMediator.Subscribe("analysisRule", getSelf()), getSelf());
        this.map = map;
    }

    public void onReceive(Object msg) {
        if (msg instanceof String)
            log.info("Got: {}", msg);
        else if (msg instanceof DistributedPubSubMediator.SubscribeAck)
            log.info("subscribing");
        else
            unhandled(msg);
    }

    public static Props props(AnalysisRuleMap database) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new AnalysisRuleMapActor(database);
            }
        });
    }


    public void handle(JsonObject json) {
        String project = json.getString("tracker");

        switch (json.getInteger("operation")) {
            case ADD:
                map.add(project, AnalysisRuleParser.parse(json.getObject("rule")));
                break;
            case DELETE:
                map.remove(project, AnalysisRuleParser.parse(json.getObject("rule")));
            case UPDATE_BATCH:
                map.updateBatch(project, AnalysisRuleParser.parse(json.getObject("rule")));
            default:
                throw new IllegalArgumentException("operation doesn't exist");
        }
    }
}
