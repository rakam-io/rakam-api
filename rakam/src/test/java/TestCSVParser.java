import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.rakam.collection.CsvEventDeserializer;
import org.rakam.collection.EventCollectionHttpService.EventList;
import org.testng.annotations.Test;

public class TestCSVParser {
    @Test
    public void testName() throws Exception {
        CsvMapper mapper = new CsvMapper();

        mapper.registerModule(new SimpleModule().addDeserializer(EventList.class, new CsvEventDeserializer(null)));
        String csv = "Transaction_date,Product,Price,Payment_Type,Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude\n" +
                "1/2/09 6:17,Product1,1200,Mastercard,carolina,Basildon,England,United Kingdom,1/2/09 6:00,1/2/09 6:08,51.5,-1.1166667\n" +
                "1/2/09 4:53,Product1,1200,Visa,Betina,Parkville                   ,MO,United States,1/2/09 4:42,1/2/09 7:49,39.195,-94.68194\n" +
                "1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea,Astoria                     ,OR,United States,1/1/09 16:21,1/3/09 12:32,46.18806,-123.83\n" +
                "1/3/09 14:44,Product1,1200,Visa,Gouya,Echuca,Victoria,Australia,9/25/05 21:13,1/3/09 14:22,-36.1333333,144.75\n";

        EventList o = mapper.reader(EventList.class).with(ContextAttributes.getEmpty()
                        .withSharedAttribute("project", "project")
                        .withSharedAttribute("collection", "collection")
                        .withSharedAttribute("api_key", "api_key")
        ).readValue(csv);

        System.out.println(o);
    }

}
