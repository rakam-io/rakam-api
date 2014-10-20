package org.rakam.collection;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/10/14 14:30.
 */
public class CustomThread extends Thread {
    private final int ia;

    public CustomThread(int ia) {
        this.ia = ia;
    }

    public CustomThread(ThreadGroup group, Runnable r, String s, int i) {
        super(group, r, s, i);
        ia = 4;
    }

    abstract static class CustomRunnable implements Runnable {
        private int ia;

        public void setIa(int ia) {
            this.ia = ia;
        }
    }
}
