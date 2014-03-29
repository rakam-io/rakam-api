package org.rakam.util;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.io.IOException;

/**
 * Created by buremba on 21/12/13.
 */

public class SpanDateTime implements com.hazelcast.nio.serialization.DataSerializable {
    private final static PeriodFormatter formatter = new PeriodFormatterBuilder()
            .appendYears().appendSuffix("year")
            .appendMonths().appendSuffix("month")
            .appendWeeks().appendSuffix("week")
            .appendDays().appendSuffix("day")
            .appendHours().appendSuffix("hour")
            .appendMinutes().appendSuffix("min")
            .toFormatter();
    private Period period;
    private DateTime spanedDateTime = null;


    public SpanDateTime(Period p) {
        this.period = p;
    }

    public SpanDateTime() {
        //
    }

    public SpanDateTime(Period p, DateTime spaned) {
        this.period = p;
        spanedDateTime = spaned;
    }

    public Integer untilTimeFrame(DateTime datetime) {
        return new Period(spanedDateTime, datetime).getMillis()/period.getMillis();
    }

    public DateTime getDateTime() {
        return spanedDateTime;
    }

    public static SpanDateTime fromPeriod(String str) {
        return new SpanDateTime(formatter.parsePeriod(str.replaceAll("\\s+", "")));
    }

    public SpanDateTime getPrevious() {
        DateTime start = new DateTime(spanedDateTime);
        return new SpanDateTime(this.period, start.minus(period));
    }

    public SpanDateTime getNext() {
        DateTime start = new DateTime(spanedDateTime);
        return new SpanDateTime(this.period, start.plus(period));
    }


    public SpanDateTime spanCurrentTimestamp() {
        return spanTimestamp(new DateTime());
    }

    public SpanDateTime spanTimestamp(Object now) {
        return spanTimestamp(new DateTime(now));
    }

    public SpanDateTime spanTimestamp(DateTime now) {
        /*
            TODO: parsePeriod usually takes 10ms and this is not good. Write a basic parser for this specific pattern.
            The other part usually takes 10ms, it doesn't that bad but I'm sure we can optimize the code.
        */
        if (spanedDateTime != null)
            return this;


        int year = now.getYear();
        Integer month = null, day = null, hour = null, minute = null;

        if (period.getYears() > 0) {
            year -= year % period.getYears();
        }

        if (period.getMonths() > 0) {
            month = now.getMonthOfYear();
            month -= month % period.getMonths();
        }

        if (period.getWeeks() > 0) {
            if (period.getDays() > 0)
                //throw new TimeFormatException("you can't use week interval with day.");
                return null;
            int day_of_m = now.getDayOfMonth();
            day = day_of_m - (day_of_m % period.getWeeks() * 7);
        }

        if (period.getDays() > 0) {
            day = now.getDayOfMonth();
            day -= day % period.getDays();
        }

        if (period.getHours() > 0) {
            hour = now.getHourOfDay();
            hour -= hour % period.getHours();
        }

        if (period.getMinutes() > 0) {
            minute = now.getMinuteOfHour();
            minute -= minute % period.getMinutes();
        }

        if (month == null)
            month = (minute != null || hour != null || day != null) ? now.getMonthOfYear() : 1;

        if (day == null)
            day = (hour != null || minute != null) ? now.getDayOfMonth() : 1;

        if (hour == null)
            hour = minute != null ? now.getHourOfDay() : 0;

        if (minute == null)
            minute = 0;
        spanedDateTime = new DateTime(year, month, day, hour, minute, 0, 0, DateTimeZone.UTC);
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(period);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        period = in.readObject();
    }
}
