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

public class TestReferrerEventMapper {
    @DataProvider(name = "google-referrer")
    public static Object[][] hashEnabledValuesProvider() throws UnknownHostException {
        return new Object[][]{
                {ImmutableMap.of("_referrer", "https://google.com/?q=test"), EventMapper.RequestParams.EMPTY_PARAMS},
                {ImmutableMap.of("_referrer", true), (EventMapper.RequestParams) () -> new DefaultHttpHeaders().set("Referer", "https://google.com/?q=test")},
                {ImmutableMap.of("_referrer", "https://google.com/?q=test"), (EventMapper.RequestParams) () -> new DefaultHttpHeaders().set("Referer", "https://google.com/?q=test")}
        };
    }

    @Test(dataProvider = "google-referrer")
    public void testReferrer(Map<String, Object> props, EventMapper.RequestParams headers) throws Exception {
        ReferrerEventMapper mapper = new ReferrerEventMapper();
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(builder.build().dependentFields.get("_referrer").stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        props.forEach(properties::put);

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, headers, InetAddress.getLocalHost(), null);

        assertEquals("Google", event.getAttribute("_referrer_source"));
        assertEquals("test", event.getAttribute("_referrer_term"));
        assertEquals("search", event.getAttribute("_referrer_medium"));
        assertEquals("google.com", event.getAttribute("_referrer_domain"));
        assertEquals("/?q=test", event.getAttribute("_referrer_path"));
        assertNull(resp);
        GenericData.get().validate(properties.getSchema(), properties);
    }

    @Test()
    public void testReferrerNotExists() throws Exception {
        ReferrerEventMapper mapper = new ReferrerEventMapper();
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        List<SchemaField> fields = builder.build().dependentFields.get("_referrer");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(fields.stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", true);

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, EventMapper.RequestParams.EMPTY_PARAMS, InetAddress.getLocalHost(), null);

        assertNull(resp);
        for (SchemaField field : fields) {
            assertNull(event.getAttribute(field.getName()));
        }
    }

    @Test()
    public void testUnknownReferrer() throws Exception {
        ReferrerEventMapper mapper = new ReferrerEventMapper();
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        List<SchemaField> fields = builder.build().dependentFields.get("_referrer");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(fields.stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", "http://test.com");

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, EventMapper.RequestParams.EMPTY_PARAMS, InetAddress.getLocalHost(), null);

        assertNull(resp);
        assertNull(event.getAttribute("_referrer_source"));
        assertNull(event.getAttribute("_referrer_term"));
        assertEquals("unknown", event.getAttribute("_referrer_medium"));
        assertEquals("test.com", event.getAttribute("_referrer_domain"));
        assertEquals("", event.getAttribute("_referrer_path"));
    }

    @Test()
    public void testDisableReferrer() throws Exception {
        ReferrerEventMapper mapper = new ReferrerEventMapper();
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        List<SchemaField> fields = builder.build().dependentFields.get("_referrer");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(fields.stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", false);

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, () -> {
            return new DefaultHttpHeaders().set("Referrer", "https://google.com/?q=test");
        }, InetAddress.getLocalHost(), null);

        assertNull(resp);
        for (SchemaField field : fields) {
            assertNull(event.getAttribute(field.getName()));
        }
    }

    @Test()
    public void testInternalReferrer() throws Exception {
        ReferrerEventMapper mapper = new ReferrerEventMapper();
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        List<SchemaField> fields = builder.build().dependentFields.get("_referrer");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(fields.stream()
                        .map(AvroUtil::generateAvroField).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null),
                        new Schema.Field("_host", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", "https://test.com/");
        properties.put("_host", "test.com");

        Event event = new Event("testproject", "testcollection", null, null, properties);

        List<Cookie> resp = mapper.map(event, EventMapper.RequestParams.EMPTY_PARAMS, InetAddress.getLocalHost(), null);

        assertNull(resp);
        assertNull(event.getAttribute("_referrer_source"));
        assertNull(event.getAttribute("_referrer_term"));
        assertEquals("internal", event.getAttribute("_referrer_medium"));
        GenericData.get().validate(properties.getSchema(), properties);
    }

    @Test
    public void testSameHostReferrer()
            throws Exception {


    }
}
