package org.rakam.util;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExportUtil {
    public static byte[] exportAsCSV(QueryResult result) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final CSVPrinter csvPrinter;
        try {
            final CSVFormat format = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.NON_NUMERIC);
            csvPrinter = new CSVPrinter(new PrintWriter(out), format);
            csvPrinter.printRecord(result.getMetadata().stream().map(SchemaField::getName)
                    .collect(Collectors.toList()));
            csvPrinter.printRecords(Iterables.transform(result.getResult(), input -> Iterables.transform(input, input1 -> {
                if (input1 instanceof List || input1 instanceof Map) {
                    return JsonHelper.encode(input1);
                }
                if (input1 instanceof byte[]) {
                    return DatatypeConverter.printBase64Binary((byte[]) input1);
                }
                return input1;
            })));
            csvPrinter.flush();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return out.toByteArray();
    }

    public static byte[] exportAsAvro(QueryResult result) {
        Schema avroSchema = AvroUtil.convertAvroSchema(result.getMetadata());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter writer = new FilteredRecordWriter(avroSchema, GenericData.get());

        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        GenericData.Record record = new GenericData.Record(avroSchema);

        for (List<Object> row : result.getResult()) {
            List<SchemaField> metadata = result.getMetadata();

            for (int i = 0; i < row.size(); i++) {
                record.put(i, getAvroValue(row.get(i), metadata.get(i).getType()));
            }

            try {
                writer.write(record, encoder);
            } catch (Exception e) {
                throw new RuntimeException("Couldn't serialize event", e);
            }
        }

        return out.toByteArray();
    }

    private static Object getAvroValue(Object value, FieldType type) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case STRING:
                return (String) value;
            case INTEGER:
                return value instanceof Integer ? value : ((Number) value).intValue();
            case LONG:
                return value instanceof Long ? value : ((Number) value).longValue();
            case BOOLEAN:
                return (Boolean) value;
            case DOUBLE:
                return value instanceof Double ? value : ((Number) value).doubleValue();
            case DATE:
                return ((LocalDate) value).toEpochDay();
            case TIMESTAMP:
                return ((Instant) value).toEpochMilli();
            case TIME:
                return ((LocalTime) value).toSecondOfDay();
            case BINARY:
                return (byte[]) value;
            default:
                if (type.isArray()) {
                    return ((List) value).stream().map(e -> getAvroValue(e, type.getArrayElementType()))
                            .collect(Collectors.toList());
                }
                if (type.isMap()) {
                    return ((Map) value).entrySet().stream()
                            .collect(Collectors.toMap(new Function<Map.Entry, String>() {
                                @Override
                                public String apply(Map.Entry entry) {
                                    return (String) entry.getKey();
                                }
                            }, e -> getAvroValue(e, type.getMapValueType())));
                }
                throw new IllegalStateException("unsupported type");
        }
    }
}
