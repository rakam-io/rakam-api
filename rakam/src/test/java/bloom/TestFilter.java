package bloom;

import java.util.UUID;

public class TestFilter {

    //    @Test
    public void testName()
            throws Exception {
        OoaBFilter byteArrayFilter = new OoaBFilter(1000000);

        while (true) {
            UUID uuid = UUID.randomUUID();
            long leastSignificantBits = uuid.getLeastSignificantBits();
            long mostSignificantBits = uuid.getMostSignificantBits();

            if (byteArrayFilter.containsAndAdd(leastSignificantBits, mostSignificantBits)) {
                throw new RuntimeException();
            }
        }
    }
}
