package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.plugin.EventMapper;
import org.rakam.server.http.annotations.ApiParam;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public interface UserPropertyMapper
{
    List<Cookie> map(String project, BatchUserOperation user, EventMapper.RequestParams requestParams, InetAddress sourceAddress);

    class BatchUserOperation
    {
        public Object id;
        public final User.UserContext api;
        public final List<Data> data;

        @JsonCreator
        public BatchUserOperation(@ApiParam("id") Object id,
                @ApiParam("api") User.UserContext api,
                @ApiParam("data") List<Data> data)
        {
            this.id = id;
            this.api = api;
            this.data = data;
        }

        public static class Data
        {
            @JsonProperty("set_properties") public final Map<String, Object> setProperties;
            @JsonProperty("set_properties_once") public final Map<String, Object> setPropertiesOnce;
            @JsonProperty("increment_properties") public final Map<String, Long> incrementProperties;
            @JsonProperty("unset_properties") public final List<String> unsetProperties;
            @JsonProperty("time") public final Long time;

            @JsonCreator
            public Data(@ApiParam("set_properties") Map<String, Object> setProperties,
                    @ApiParam("set_properties_once") Map<String, Object> setPropertiesOnce,
                    @ApiParam("increment_properties") Map<String, Long> incrementProperties,
                    @ApiParam("unset_properties") List<String> unsetProperties,
                    @ApiParam("time") Long time)
            {
                this.setProperties = setProperties;
                this.setPropertiesOnce = setPropertiesOnce;
                this.incrementProperties = incrementProperties;
                this.unsetProperties = unsetProperties;
                this.time = time;
            }
        }

        public void setId(Object id) {
            this.id = id;
        }
    }
}

