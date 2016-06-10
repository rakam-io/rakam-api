package org.rakam.util;

import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by buremba on 6/10/16.
 */
public class ExportUtil
{
    public static byte[] exportAsCSV(QueryResult result)
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final CSVPrinter csvPrinter;
        try {
            csvPrinter = new CSVPrinter(new PrintWriter(out), CSVFormat.DEFAULT);
            csvPrinter.printRecord(result.getMetadata().stream().map(SchemaField::getName).collect(Collectors.toList()));
            csvPrinter.printRecords(result.getResult());
            csvPrinter.flush();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return out.toByteArray();
    }

    public static byte[] exportAsAvro(QueryResult result)
    {
        Schema avroSchema = AvroUtil.convertAvroSchema(result.getMetadata());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter writer = new FilteredRecordWriter(avroSchema, GenericData.get());

        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        GenericData.Record record = new GenericData.Record(avroSchema);

        for (List<Object> row : result.getResult()) {

            for (int i = 0; i < row.size(); i++) {
                record.put(i, row.get(i));
            }

            try {
                writer.write(record, encoder);
            }
            catch (Exception e) {
                throw new RuntimeException("Couldn't serialize event", e);
            }
        }

        return out.toByteArray();
    }
}
