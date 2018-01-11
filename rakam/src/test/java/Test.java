import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.io.ByteStreams;
import org.rakam.util.javascript.ILogger;
import org.rakam.util.javascript.JSCodeCompiler;
import org.rakam.util.javascript.JSLoggerService;
import org.rakam.util.javascript.JavascriptConfig;

import javax.script.Invocable;
import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main1(String[] args) throws IOException, ScriptException, NoSuchMethodException {
        JSCodeCompiler jsCodeCompiler = new JSCodeCompiler(null, null, new JSLoggerService() {
            @Override
            public ILogger createLogger(String project, String prefix) {
                return new JSCodeCompiler.TestLogger();
            }

            @Override
            public ILogger createLogger(String project, String prefix, String identifier) {
                return new JSCodeCompiler.TestLogger();
            }
        }, new JavascriptConfig());


        Invocable engine = jsCodeCompiler.createEngine("test", new String(ByteStreams.toByteArray(Test.class.getResourceAsStream("example.js"))), "test");
        HashMap<Object, Object> map = new HashMap<>();
        HashMap<Object, Object> inline = new HashMap<>();
        inline.put("param", 1);
        map.put("test", inline);
        Object main = engine.invokeFunction("main", map);
        System.out.println(main);
    }

    public static void main(String[] args) throws IOException, ScriptException, NoSuchMethodException {
        CsvMapper mapper = new CsvMapper();

        CsvSchema.Builder schema = new CsvSchema.Builder();
        schema.addColumn("geonameid", CsvSchema.ColumnType.NUMBER);
        schema.addColumn("name", CsvSchema.ColumnType.STRING);
        schema.addColumn("asciiname", CsvSchema.ColumnType.STRING);
        schema.addColumn("alternatenames", CsvSchema.ColumnType.STRING);
        schema.addColumn("latitude", CsvSchema.ColumnType.NUMBER);
        schema.addColumn("longitude", CsvSchema.ColumnType.NUMBER);
        schema.addColumn("feature class", CsvSchema.ColumnType.STRING);
        schema.addColumn("feature code", CsvSchema.ColumnType.STRING);
        schema.addColumn("country code", CsvSchema.ColumnType.STRING);
        schema.addColumn("cc2", CsvSchema.ColumnType.STRING);
        schema.addColumn("admin1 code", CsvSchema.ColumnType.STRING);
        schema.addColumn("admin2 code", CsvSchema.ColumnType.STRING);
        schema.addColumn("admin3 code", CsvSchema.ColumnType.STRING);
        schema.addColumn("admin4 code", CsvSchema.ColumnType.STRING);
        schema.addColumn("population", CsvSchema.ColumnType.STRING);
        schema.addColumn("elevation", CsvSchema.ColumnType.STRING);
        schema.addColumn("dem", CsvSchema.ColumnType.STRING);
        schema.addColumn("timezone", CsvSchema.ColumnType.STRING);
        schema.addColumn("modification date", CsvSchema.ColumnType.STRING);
        schema.setColumnSeparator('\t');
        schema.setEscapeChar('"');

        File csvFile = new File("/Users/buremba/Downloads/allCountries.txt"); // or from String, URL etc
        MappingIterator<Map> it = mapper.readerFor(Map.class).with(schema.build()).readValues(csvFile);
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
//            System.out.println(row.get("latitude"));
        }
    }
}
