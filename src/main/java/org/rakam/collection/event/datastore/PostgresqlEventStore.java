package org.rakam.collection.event.datastore;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.collection.event.EventStore;
import org.rakam.model.Event;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 16:22.
 */
@Singleton
public class PostgresqlEventStore implements EventStore {

    @Inject
    public PostgresqlEventStore(PostgresqlConfig postgresqlConfig) {

    }

    @Override
    public void store(Event event) {

    }

}
