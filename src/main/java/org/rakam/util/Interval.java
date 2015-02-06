package org.rakam.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.rakam.util.TimeUtil.UTCTime;

/**
 * Created by buremba on 21/12/13.
 */

public abstract class Interval implements JsonSerializable {
    final static Pattern parser = Pattern.compile("^([0-9]+)([a-z]+)s?$");
    public static final Interval MINUTE = new TimeSpan(Duration.ofMinutes(1));
    public static final Interval HOUR = new TimeSpan(Duration.ofHours(1));
    public static final Interval SECOND = new TimeSpan(Duration.ofSeconds(1));
    public static final Interval DAY = new TimeSpan(Duration.ofDays(1));
    public static final Interval WEEK = new TimeSpan(Duration.ofDays(7));
    public static final Interval MONTH = new MonthSpan(Period.ofMonths(1));

    public abstract StatefulSpanTime span(int time);

    public StatefulSpanTime spanCurrent() {
        return span(UTCTime());
    }

    public abstract boolean isDivisible(Interval interval);

    public abstract long divide(Interval interval);

    public static Interval parse(String str) throws IllegalArgumentException {
        Matcher match = parser.matcher(str);
        if (match.find()) {
            int num = Integer.parseInt(match.group(1));
            // TODO: switch to String.indexOf
            switch (match.group(2)) {
                case "months":
                case "month":
                    return new MonthSpan(Period.ofMonths(num));
                case "weeks":
                case "week":
                    return new TimeSpan(Duration.ofDays(7 * num));
                case "day":
                case "days":
                    return new TimeSpan(Duration.ofDays(num));
                case "hours":
                case "hour":
                    return new TimeSpan(Duration.ofHours(num));
                case "minutes":
                case "minute":
                    return new TimeSpan(Duration.ofMinutes(num));
            }
        }
        throw new IllegalArgumentException("couldn't parse interval string. usage [*month, *week, *day, *hour, *minute], ");
    }

    public static class MonthSpan extends Interval {
        private final Period period;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MonthSpan)) return false;

            MonthSpan monthSpan = (MonthSpan) o;

            if (!period.equals(monthSpan.period)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return period.hashCode();
        }

        @Override
        public StatefulSpanTime span(int time) {
            return new StatefulMonthSpan(time);
        }

        @Override
        public boolean isDivisible(Interval interval) {
            if (interval instanceof MonthSpan) {
                return period.toTotalMonths() % ((MonthSpan) interval).period.toTotalMonths() == 0;
            }
            if (interval instanceof TimeSpan) {
                return new TimeSpan(Duration.ofDays(1)).isDivisible(interval);
            }
            return false;
        }

        @Override
        public long divide(Interval interval) {
            if (interval instanceof MonthSpan) {
                return period.toTotalMonths() % ((MonthSpan) interval).period.toTotalMonths();
            }
            throw new IllegalArgumentException();
        }

        public MonthSpan(Period p) {
            this.period = p;
        }

        @Override
        public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeString(period.toTotalMonths() + "months");
        }

        @Override
        public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException, JsonProcessingException {

        }

        public class StatefulMonthSpan implements StatefulSpanTime, Serializable {
            private LocalDateTime cursor;
            private final static int DAY = 24 * 60 * 60;


            public StatefulMonthSpan(int cursor) {
                final LocalDate firstDay = LocalDate.ofEpochDay(cursor / DAY).withDayOfMonth(1);
                int numberOfMonths = firstDay.getYear() * 12 + firstDay.getMonthValue();
                final long epoch = firstDay.minusMonths(numberOfMonths % period.toTotalMonths()).toEpochDay() * DAY;
                this.cursor = LocalDateTime.ofEpochSecond(epoch, 100_000_000, ZoneOffset.UTC);
            }

            public int current() {
                return (int) cursor.toEpochSecond(ZoneOffset.UTC);
            }

            public long untilTimeFrame(int now) {
                final LocalDate localDate = LocalDate.ofEpochDay(now / DAY);
                int numberOfMonths1 = localDate.getYear() * 12 + localDate.getMonthValue();

                int numberOfMonths2 = cursor.getYear() * 12 + cursor.getMonthValue();
                return (numberOfMonths2 - numberOfMonths1) / period.toTotalMonths();
            }

            public StatefulSpanTime next() {
                cursor = cursor.plus(period);
                return this;
            }

            public StatefulSpanTime previous() {
                cursor = cursor.minus(period);
                return this;
            }


        }

    }

    public static class TimeSpan extends Interval implements JsonSerializable {
        private int period;

        @JsonCreator
        public TimeSpan(Duration duration) {
            this.period = (int) duration.getSeconds();
        }

        @Override
        public String toString() {
            return "TimeSpan{" + "period=" + period + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TimeSpan)) return false;

            TimeSpan timeSpan = (TimeSpan) o;

            if (period != timeSpan.period) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return period;
        }

        @Override
        public StatefulSpanTime span(int time) {
            return new StatefulTimeSpan(time);
        }

        @Override
        public boolean isDivisible(Interval interval) {
            if (interval instanceof TimeSpan) {
                return period % ((TimeSpan) interval).period == 0;
            }
            return false;
        }

        @Override
        public long divide(Interval interval) {
            if (interval instanceof TimeSpan && this.isDivisible(interval)) {
                return period / ((TimeSpan) interval).period;
            }
            throw new IllegalStateException();
        }

        @Override
        public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException {
            StringBuilder str = new StringBuilder();
            int p = period;
            if (p >= 86400) {
                str.append(p / 86400 + "days");
                p = period % 86400;
            }

            if (p >= 3600) {
                str.append(p / 3600 + "hours");
                p = period % 3600;
            }

            if (p >= 60) {
                str.append(p / 60 + "minutes");
                p = period % 60;
            }

            if (p > 0) {
                str.append(p + "seconds");
            }

            jgen.writeString(str.toString());
        }

        @Override
        public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer) throws IOException, JsonProcessingException {
            System.out.println(".");
        }

        public class StatefulTimeSpan implements StatefulSpanTime, Serializable {
            private int cursor;

            public StatefulTimeSpan(int cursor) {
                this.cursor = (cursor / period) * period;
            }

            public int current() {
                return cursor;
            }

            public long untilTimeFrame(int now) {
                return (now - cursor) / period;
            }

            public StatefulSpanTime next() {
                cursor += period;
                return this;
            }

            public StatefulSpanTime previous() {
                cursor -= period;
                return this;
            }
        }
    }

    public interface StatefulSpanTime {
        abstract public long untilTimeFrame(int frame);

        abstract public int current();

        abstract public StatefulSpanTime next();

        abstract public StatefulSpanTime previous();
    }
}
