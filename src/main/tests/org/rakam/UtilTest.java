package org.rakam;

import org.junit.Assert;
import org.junit.Test;
import org.rakam.stream.AverageCounter;
import org.rakam.util.Interval;
import org.rakam.util.MMapFile;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.rakam.util.Lambda.produceLambda;

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
    public void voidMethodToLambda() throws Throwable {

        final MethodHandles.Lookup lookup = MethodHandles.lookup();

        final BiConsumer setterLambda = produceLambda(lookup, Target3.class.getDeclaredMethod("id", int.class), BiConsumer.class.getMethod("accept", Object.class, Object.class));

        setterLambda.accept(new Target3(123), 456);

        final Function<Target3, Integer> getterLambda = produceLambda(lookup, Target3.class.getDeclaredMethod("id"), Function.class.getMethod("apply", Object.class));

        System.out.println(getterLambda.apply(new Target3(123)));
    }

}
