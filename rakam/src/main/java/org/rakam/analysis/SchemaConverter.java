package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.AvroUtil;
import org.rakam.util.RakamException;

import java.util.*;
import java.util.function.Function;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.apache.avro.Schema.Type.NULL;

public enum SchemaConverter {
    AVRO(schemaFields -> {
        Schema parse = new Schema.Parser().parse(schemaFields);
        if (parse.getType() != Schema.Type.RECORD) {
            throw new RakamException("Avro schema must be a RECORD", BAD_REQUEST);
        }

        Set<SchemaField> rakamFields = new HashSet<>();

        for (Schema.Field field : parse.getFields()) {
            Schema avroSchema = field.schema();
            if (avroSchema.getType() == Schema.Type.UNION) {
                List<Schema> types = field.schema().getTypes();
                if (types.isEmpty()) {
                    throw new IllegalStateException();
                }
                if (types.size() == 1) {
                    avroSchema = types.get(0);
                }
                if (types.size() == 2) {
                    if (types.get(0).getType() == Schema.Type.NULL) {
                        avroSchema = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), types.get(1)));
                    } else if (types.get(1).getType() == Schema.Type.NULL) {
                        avroSchema = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), types.get(0)));
                    } else {
                        throw new RakamException("UNION type is not supported: " + avroSchema, BAD_REQUEST);
                    }
                } else {
                    throw new RakamException("UNION type is not supported: " + avroSchema, BAD_REQUEST);
                }
            }

            final Schema finalAvroSchema = avroSchema;
            Optional<FieldType> fieldType = Arrays.stream(FieldType.values()).filter(e -> AvroUtil.generateAvroSchema(e).equals(finalAvroSchema)).findAny();
            if (!fieldType.isPresent()) {
                new RakamException("Unsupported Avro type" + avroSchema, BAD_REQUEST);
            }

            rakamFields.add(new SchemaField(field.name(), fieldType.get(), avroSchema.getFullName(), avroSchema.getDoc(), null));
        }
        return rakamFields;
    });

    private final Function<String, Set<SchemaField>> mapper;

    SchemaConverter(Function<String, Set<SchemaField>> mapper) {
        this.mapper = mapper;
    }

    @JsonCreator
    public static SchemaConverter get(String name) {
        return valueOf(name.toUpperCase());
    }

    @JsonProperty
    public String value() {
        return name();
    }

    public Function<String, Set<SchemaField>> getMapper() {
        return mapper;
    }
}
