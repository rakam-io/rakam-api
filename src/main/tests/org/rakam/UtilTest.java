package org.rakam;

import org.junit.Assert;
import org.junit.Test;
import org.rakam.stream.AverageCounter;
import org.rakam.util.Interval;
import org.rakam.util.MMapFile;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/09/14 15:38.
 */
public class UtilTest {
    @Test
    public void spanTimeTest() {
        Assert.assertEquals(Interval.parse("1d"), 60 * 60 * 24);
    }

    @Test
    public void chronicle() throws IOException, InterruptedException {
        MMapFile mMapFile = new MMapFile("/tmp/naber/ali.index", READ_ONLY, 32 * 1024 * 1024);
        int idx = 0;
        AverageCounter averageCounter = new AverageCounter();
        while(true) {
            int i = mMapFile.readInt();
            averageCounter.increment(i);
            if(idx++ % 10000 == 0)
                System.out.println(idx+" "+averageCounter.getValue());
        }
    }


    @Test
    public void chfronicle() throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.execute(() -> {
            System.out.println(1);

            executorService.execute(() -> System.out.println(2));

            System.out.println(3);
        });

        executorService.awaitTermination(10000, TimeUnit.SECONDS);
    }
}
