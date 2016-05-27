package org.rakam;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.net.InetAddress;
import java.util.List;

public class RakamUserIdEventMapper implements EventMapper {
    public RakamUserIdEventMapper() {
        Jedis jedis = new Jedis("localhost");
        Pipeline p = jedis.pipelined();
        p.set();
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress) {
        return null;
    }
}
