package org.rakam.module.website;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.collection.Event;
import org.rakam.collection.SchemaField;
import org.rakam.collection.FieldDependencyBuilder;
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
        return new Object[][] {
                { ImmutableMap.of("_referrer", "https://google.com/?q=test"), HttpHeaders.EMPTY_HEADERS },
                { ImmutableMap.of("_referrer", true), new DefaultHttpHeaders().set("Referer", "https://google.com/?q=test") },
                { ImmutableMap.of("_referrer", "https://google.com/?q=test"), new DefaultHttpHeaders().set("Referer", "https://google.com/?q=test")  }
        };
    }

    @Test(dataProvider = "google-referrer")
    public void testReferrer(Map<String, Object> props, HttpHeaders headers) throws Exception {
        ReferrerEventMapper mapper = new ReferrerEventMapper();
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        builder.build();

        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(builder.build().dependentFields.get("_referrer").stream()
                        .map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        props.forEach(properties::put);

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, headers, InetAddress.getLocalHost());

        assertEquals("Google", event.getAttribute("referrer_source"));
        assertEquals("test", event.getAttribute("referrer_term"));
        assertEquals("search", event.getAttribute("referrer_medium"));
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
                        .map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", true);

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, InetAddress.getLocalHost());

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
                        .map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", "http://test.com");

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, InetAddress.getLocalHost());

        assertNull(resp);
        assertNull(event.getAttribute("referrer_source"));
        assertNull(event.getAttribute("referrer_term"));
        assertEquals("unknown", event.getAttribute("referrer_medium"));
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
                        .map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", false);

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, new DefaultHttpHeaders().set("Referrer", "https://google.com/?q=test"),
                InetAddress.getLocalHost());

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
                        .map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_referrer", Schema.create(NULL), null, null), new Schema.Field("_host", Schema.create(NULL), null, null))
                .build();

        GenericData.Record properties = new GenericData.Record(Schema.createRecord(build));
        properties.put("_referrer", "https://test.com/");
        properties.put("_host", "test.com");

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, InetAddress.getLocalHost());

        assertNull(resp);
        assertNull(event.getAttribute("referrer_source"));
        assertNull(event.getAttribute("referrer_term"));
        assertEquals("internal", event.getAttribute("referrer_medium"));
        GenericData.get().validate(properties.getSchema(), properties);
    }
}
