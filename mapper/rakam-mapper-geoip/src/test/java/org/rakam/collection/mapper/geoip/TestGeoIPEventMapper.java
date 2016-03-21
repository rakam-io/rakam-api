package org.rakam.collection.mapper.geoip;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;
import static org.testng.Assert.*;

public class TestGeoIPEventMapper {
    @DataProvider(name = "google-ips")
    public static Object[][] hashEnabledValuesProvider() throws UnknownHostException {
        return new Object[][] {
                { ImmutableMap.of("_ip", "8.8.8.8"), InetAddress.getLocalHost() },
                { ImmutableMap.of("_ip", true), InetAddress.getByName("8.8.8.8")  },
                { ImmutableMap.of("_ip", "8.8.8.8"), InetAddress.getByName("8.8.8.8")  }
        };
    }

    @Test(dataProvider = "google-ips")
    public void testIspEventMapper(Map<String, Object> props, InetAddress address) throws Exception {
        GeoIPEventMapper mapper = new GeoIPEventMapper(new GeoIPModuleConfig()
                .setAttributes("")
                .setIspDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-ISP-Test.mmdb"));
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        Record properties = new Record(Schema.createRecord(ImmutableList.of(
                new Schema.Field("_ip", Schema.create(NULL), null, null),
                new Schema.Field("isp", Schema.create(STRING), null, null))));
        props.forEach(properties::put);

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, address);

        assertTrue(resp == null);

        assertEquals(event.getAttribute("isp"), "Level 3 Communications");
        GenericData.get().validate(properties.getSchema(), properties);
    }

    @Test(dataProvider = "google-ips")
    public void testConnectionTypeEventMapper(Map<String, Object> props, InetAddress address) throws Exception {
        GeoIPEventMapper mapper = new GeoIPEventMapper(new GeoIPModuleConfig()
                .setAttributes("")
                .setConnectionTypeDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-Connection-Type-Test.mmdb"));
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        Record properties = new Record(Schema.createRecord(ImmutableList.of(
                new Schema.Field("_ip", Schema.create(NULL), null, null),
                new Schema.Field("connection_type", Schema.create(STRING), null, null))));
        props.forEach(properties::put);

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, address);

        assertTrue(resp == null);

        // TODO: find a reliable ip that can be mapped.
        assertNull(event.getAttribute("connection_type"));
        GenericData.get().validate(properties.getSchema(), properties);
    }

    @Test(dataProvider = "google-ips")
    public void testEventMapper(Map<String, Object> props, InetAddress address) throws Exception {
        GeoIPEventMapper mapper = new GeoIPEventMapper(new GeoIPModuleConfig());
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(builder.build().dependentFields.get("_ip").stream().map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_ip", Schema.create(NULL), null, null))
                .build();

        Record properties = new Record(Schema.createRecord(build));
        props.forEach(properties::put);

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, address);

        assertTrue(resp == null);

        assertEquals(event.getAttribute("country_code"), "US");
        assertEquals(event.getAttribute("city"), "Mountain View");
        assertEquals(event.getAttribute("timezone"), "America/Los_Angeles");
        assertEquals(event.getAttribute("latitude"), 37.3845);
        assertEquals(event.getAttribute("region"), "North America");
        assertEquals(event.getAttribute("longitude"), -122.0881);
        GenericData.get().validate(properties.getSchema(), properties);
    }

    @Test
    public void testNotFoundIpEventMapper() throws Exception {
        GeoIPEventMapper mapper = new GeoIPEventMapper(new GeoIPModuleConfig()
                .setConnectionTypeDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-Connection-Type-Test.mmdb")
                .setIspDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-ISP-Test.mmdb"));
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        List<SchemaField> ip = builder.build().dependentFields.get("_ip");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(ip.stream().map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_ip", Schema.create(NULL), null, null))
                .build();

        Record properties = new Record(Schema.createRecord(build));
        properties.put("_ip", "127.0.0.1");

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, InetAddress.getLocalHost());

        assertTrue(resp == null);
        for (SchemaField schemaField : ip) {
            assertNull(event.getAttribute(schemaField.getName()));
        }
    }

    @Test
    public void testNotTrackFlagIpEventMapper() throws Exception {
        GeoIPEventMapper mapper = new GeoIPEventMapper(new GeoIPModuleConfig()
                .setConnectionTypeDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-Connection-Type-Test.mmdb")
                .setIspDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-ISP-Test.mmdb"));
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        List<SchemaField> ip = builder.build().dependentFields.get("_ip");
        ImmutableList<Schema.Field> build = ImmutableList.<Schema.Field>builder()
                .addAll(ip.stream().map(AvroUtil::generateAvroSchema).collect(Collectors.toList()))
                .add(new Schema.Field("_ip", Schema.create(NULL), null, null))
                .build();

        Record properties = new Record(Schema.createRecord(build));
        properties.put("_ip", false);

        Event event = new Event("testproject", "testcollection", null, properties);

        List<Cookie> resp = mapper.map(event, HttpHeaders.EMPTY_HEADERS, InetAddress.getByName("8.8.8.8"));

        assertTrue(resp == null);
        for (SchemaField schemaField : ip) {
            assertNull(event.getAttribute(schemaField.getName()));
        }
    }

    @Test
    public void testFieldDependency() throws Exception {
        Set<String> list = ImmutableSet.of("city", "region", "country_code", "latitude", "longitude", "timezone");

        GeoIPModuleConfig config = new GeoIPModuleConfig().setAttributes(list.stream().collect(Collectors.joining(",")));
        GeoIPEventMapper mapper = new GeoIPEventMapper(config);
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        FieldDependencyBuilder.FieldDependency build = builder.build();
        assertEquals(0, build.constantFields.size());
        assertEquals(1, build.dependentFields.size());

        assertEquals(list, build.dependentFields.get("_ip").stream().map(SchemaField::getName).collect(Collectors.toSet()));
    }

    @Test
    public void testFieldDependencyWithAll() throws Exception {

        Set<String> list = ImmutableSet.of("city", "region", "country_code", "latitude", "longitude", "timezone");

        GeoIPModuleConfig config = new GeoIPModuleConfig()
                .setAttributes(list.stream().collect(Collectors.joining(",")))
                .setConnectionTypeDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-Connection-Type-Test.mmdb")
                .setIspDatabaseUrl("https://github.com/maxmind/MaxMind-DB/raw/master/test-data/GeoIP2-ISP-Test.mmdb");

        GeoIPEventMapper mapper = new GeoIPEventMapper(config);
        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        mapper.addFieldDependency(builder);

        FieldDependencyBuilder.FieldDependency build = builder.build();
        assertEquals(0, build.constantFields.size());
        assertEquals(1, build.dependentFields.size());

        assertEquals(ImmutableSet.builder().addAll(list).add("isp", "connection_type").build(),
                build.dependentFields.get("_ip").stream().map(SchemaField::getName).collect(Collectors.toSet()));
    }
}
