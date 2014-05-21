package org.rakam.util;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by buremba on 21/12/13.
 */

public class SpanTime implements com.hazelcast.nio.serialization.DataSerializable {
    private final static Pattern parser = Pattern.compile("^(?:([0-9]+)(month)s?)? ?(?:([0-9]+)(week)s?)? ?(?:([0-9]+)(day)s?)? ?(?:([0-9]+)(hour)s?)? ?(?:([0-9]+)(min)s?)?$");

    public int period;
    private int cursor = -1;


    public SpanTime(int p) {
        this.period = p;
    }

    public SpanTime(int p, int cursor) {
        this.period = p;
        this.cursor = cursor;
    }

    public int current() {
        return cursor;
    }

    public long untilTimeFrame(int now) {
        return (now - cursor) / period;
    }

    public static SpanTime fromPeriod(String str) {
        Matcher match = parser.matcher(str);
        int p = 0;
        if (match.find())
            for (int i = 1; i < 10; i+=2) {
                String num_str = match.group(i);
                if (num_str != null) {
                    int num = Integer.parseInt(num_str);
                    String period = match.group(i + 1);
                    if (period.equals("month")) {
                        p += 60 * 60 * 24 * 7 * 30 * num;
                    } else if (period.equals("week")) {
                        p += 60 * 60 * 24 * 7 * num;
                    } else if (period.equals("day")) {
                        p += 60 * 60 * 24 * num;
                    } else if (period.endsWith("hour")) {
                        p += 60 * 60 * num;
                    } else if (period.equals("min")) {
                        p += 60 * num;
                    }
                }
            }
        if (p == 0)
                throw new IllegalArgumentException("couldn't parse interval string");
        return new SpanTime(p);
    }

    public SpanTime getPrevious() {
        if (cursor == -1)
            throw new IllegalStateException("you must set cursor timestamp before using this method");
        cursor -= period;
        return this;
    }

    public SpanTime getNext() {
        if (cursor == -1)
            throw new IllegalStateException("you must set cursor timestamp before using this method");
        cursor += period;
        return this;
    }

    public String toString() {
        return Long.toString(period);
    }


    public SpanTime spanCurrent() {
        return span((int) (System.currentTimeMillis() / 1000));
    }

    public SpanTime span(int now) {
        cursor = (now / period) * period;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(period);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        period = in.readInt();
    }
}
