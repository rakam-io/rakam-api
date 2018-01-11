package org.rakam.module.website;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventMapper;
import org.rakam.util.AvroUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestUserAgentEventMapper {
    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36";

    @DataProvider(name = "chrome-user-agent")
    public static Object[][] hashEnabledValuesProvider() throws UnknownHostException {
        return new Object[][]{
                {ImmutableMap.of("_user_agent", USER_AGENT), EventMapper.RequestParams.EMPTY_PARAMS},
                {ImmutableMap.of("_user_agent", true), (EventMapper.RequestParams) () -> new DefaultHttpHeaders().set("User-Agent", USER_AGENT)},
                {ImmutableMap.of("_user_agent", USER_AGENT), (EventMapper.RequestParams) () -> new DefaultHttpHeaders().set("User-Agent", USER_AGENT)}
        };
    }

    @Test(dataProvider = "chrome-user-agent")
    public void testUserAgentMapper(Map<String, Object> props, EventMapper.RequestParams headers) throws Exception {
        UserAgentEventMapper mapper = new UserAgentEventMapper(new WebsiteMapperConfig());
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(builder.build().dependentFields.get("_user_agent").stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_user_agent", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        props.forEach(properties::put);

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, headers, InetAddress.getLocalHost(), null);

        assertEquals("Chrome", event.getAttribute("_user_agent_family"));
        assertEquals("Mac OS X", event.getAttribute("_os"));
        assertEquals("10", event.getAttribute("_os_version"));
        assertEquals("Other", event.getAttribute("_device_family"));
        assertNull(resp);
        GenericData.get().validate(properties.getSchema(), properties);
    }

    @Test()
    public void testUserAgentNotExists() throws Exception {
        UserAgentEventMapper mapper = new UserAgentEventMapper(new WebsiteMapperConfig());
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        List<SchemaField> fields = builder.build().dependentFields.get("_user_agent");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(fields.stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_user_agent", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_user_agent", true);

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, EventMapper.RequestParams.EMPTY_PARAMS, InetAddress.getLocalHost(), null);

        assertNull(resp);
        for (SchemaField field : fields) {
            assertNull(event.getAttribute(field.getName()));
        }
    }

    @Test()
    public void testUnknownUserAgent() throws Exception {
        UserAgentEventMapper mapper = new UserAgentEventMapper(new WebsiteMapperConfig());
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        List<SchemaField> fields = builder.build().dependentFields.get("_user_agent");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(fields.stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_user_agent", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_user_agent", "unknown user agent");

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, EventMapper.RequestParams.EMPTY_PARAMS, InetAddress.getLocalHost(), null);

        assertNull(resp);
        assertEquals("Other", event.getAttribute("_user_agent_family"));
        assertNull(event.getAttribute("_user_agent_version"));
        assertEquals("Other", event.getAttribute("_os"));
        assertNull(event.getAttribute("_os_version"));
        assertEquals("Other", event.getAttribute("_device_family"));
    }

    @Test()
    public void testDisableUserAgent() throws Exception {
        UserAgentEventMapper mapper = new UserAgentEventMapper(new WebsiteMapperConfig());
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        List<SchemaField> fields = builder.build().dependentFields.get("_user_agent");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(fields.stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_user_agent", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_user_agent", false);

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, () -> new DefaultHttpHeaders().set("User-Agent", USER_AGENT),
                InetAddress.getLocalHost(), null);

        assertNull(resp);
        for (SchemaField field : fields) {
            assertNull(event.getAttribute(field.getName()));
        }
    }
}
