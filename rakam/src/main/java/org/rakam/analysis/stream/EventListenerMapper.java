package org.rakam.analysis.stream;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.Mapper;
import org.rakam.analysis.stream.APIEventStreamModule.CollectionStreamHolder;
import org.rakam.analysis.stream.APIEventStreamModule.CollectionStreamHolder.CollectionFilter;
import org.rakam.collection.Event;
import org.rakam.plugin.SyncEventMapper;

import javax.inject.Inject;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

@Mapper(name = "Event stream module listener", description = "An internal event mapper that sends matching events to the API request")
public class EventListenerMapper
        implements SyncEventMapper {
    Map<String, List<CollectionStreamHolder>> lists;

    @Inject
    public EventListenerMapper(Map<String, List<CollectionStreamHolder>> lists) {
        this.lists = lists;
    }

    @Override
    public List<Cookie> map(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        List<CollectionStreamHolder> streamHolder = lists.get(event.project());

        if (streamHolder == null) {
            return null;
        }

        for (int i = 0; i < streamHolder.size(); i++) {
            // Instead of iterating, we use indices and avoid any read locks in concurrent environment.
            CollectionStreamHolder holderItem = streamHolder.get(i);
            if (holderItem == null) {
                continue;
            }

            for (CollectionFilter item : holderItem.collections) {
                if (item.collection != null) {
                    if (item.collection != null && !item.collection.equals(event.collection())) {
                        continue;
                    }
                }

                if (item.filter != null) {
                    if (!item.filter.test(event.properties())) {
                        continue;
                    }
                }

                holderItem.messageQueue.offer(event);
                break;
            }
        }

        return null;
    }
}
