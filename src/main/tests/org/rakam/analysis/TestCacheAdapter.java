package org.rakam.analysis;

import org.rakam.cache.local.LocalCacheAdapter;

import java.util.concurrent.CountDownLatch;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 17:18.
 */
public class TestCacheAdapter extends LocalCacheAdapter {
    CountDownLatch latch = null;

    public CountDownLatch listenFlush() {
        if(latch!=null) {
            return latch;
        }
        latch = new CountDownLatch(1);
        return latch;
    }

    @Override
    public void flush() {
        super.flush();
        if(latch!=null) {
            latch.countDown();
        }
    }
}
