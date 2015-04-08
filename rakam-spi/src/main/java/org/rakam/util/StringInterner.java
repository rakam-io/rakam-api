package org.rakam.util;


/**
 * @author peter.lawrey
 */
public class StringInterner {
    private final String[] interner;
    private final int mask;

    public StringInterner(int capacity) {
        int n = nextPower2(capacity, 128);
        interner = new String[n];
        mask = n - 1;
    }

    public static boolean isEqual(CharSequence s, CharSequence cs) {
        if (s == null) return false;
        if (s.length() != cs.length()) return false;
        for (int i = 0; i < cs.length(); i++)
            if (s.charAt(i) != cs.charAt(i))
                return false;
        return true;
    }

    public static int nextPower2(int n, int min) {
        if (!isPowerOf2(min))
            throw new IllegalArgumentException();
        if (n < min) return min;
        if (isPowerOf2(n))
            return n;
        int i = min;
        while (i < n) {
            i *= 2;
            if (i <= 0) return 1 << 30;
        }
        return i;
    }

    public static boolean isPowerOf2(int n) {
        return (n & (n - 1)) == 0;
    }

    public String intern(CharSequence cs) {
        long hash = 0;
        for (int i = 0; i < cs.length(); i++)
            hash = 57 * hash + cs.charAt(i);
        int h = (int) hash(hash) & mask;
        String s = interner[h];
        if (isEqual(s, cs))
            return s;
        String s2 = cs.toString();
        return interner[h] = s2;
    }

    public static long hash(long n) {
        n ^= (n >>> 41) - (n >>> 21);
        n ^= (n >>> 15) + (n >>> 7);
        return n;
    }
}