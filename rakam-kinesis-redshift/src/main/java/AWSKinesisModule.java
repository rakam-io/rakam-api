import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import org.rakam.analysis.AWSConfig;
import org.rakam.analysis.AWSKinesisEventStore;
import org.rakam.analysis.AWSKinesisEventStream;
import org.rakam.analysis.KinesisStreamWorkerManager;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.RakamModule;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 06:40.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config="event.store", value="kinesis")
public class AWSKinesisModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        AWSConfig awsConfig = buildConfigObject(AWSConfig.class);
        binder.bind(EventStore.class).to(AWSKinesisEventStore.class).in(Scopes.SINGLETON);
        binder.bind(EventStream.class).to(AWSKinesisEventStream.class).in(Scopes.SINGLETON);
        if(awsConfig.getKinesisWorker()) {
            binder.bind(KinesisStreamWorkerManager.class).asEagerSingleton();
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }
}
