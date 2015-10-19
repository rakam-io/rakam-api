package org.rakam.plugin;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.rakam.util.ProjectCollection;

import java.util.Set;

public class EventBusSystemEventListener {
    private final Logger LOGGER = Logger.get(EventBusSystemEventListener.class);

    private final Set<SystemEventListener> listeners;

    @Inject
    public EventBusSystemEventListener(Set<SystemEventListener> listeners) {
        this.listeners = listeners;
    }

    public void listenCreateCollection(ProjectCollection projectCollection) {
        for (SystemEventListener listener : listeners) {
            try {
                listener.onCreateCollection(projectCollection.project, projectCollection.collection);
            } catch (Exception e) {
                LOGGER.error(e, "Error while processing event listener");
            }
        }
    }
}
