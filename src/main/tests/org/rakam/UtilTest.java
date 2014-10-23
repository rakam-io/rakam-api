package org.rakam;

import org.junit.Assert;
import org.junit.Test;
import org.rakam.util.Interval;

import java.util.concurrent.locks.StampedLock;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/09/14 15:38.
 */
public class UtilTest {
    @Test
    public void spanTimeTest() {
        Assert.assertEquals(Interval.parse("1d"), 60 * 60 * 24);
    }
    @Test
    public void test() {

            final StampedLock stampedLock = new StampedLock();
        stampedLock.tryOptimisticRead();
    }
}
