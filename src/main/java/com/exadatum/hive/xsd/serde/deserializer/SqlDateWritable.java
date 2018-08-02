package com.exadatum.hive.xsd.serde.deserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class SqlDateWritable implements WritableComparable<SqlDateWritable> {

    private Date date = new Date();

    /* Constructors */
    public SqlDateWritable() {
    }

    public SqlDateWritable(SqlDateWritable d) {
        set(d);
    }

    public SqlDateWritable(Date d) {
        set(d);
    }

    public SqlDateWritable(int d) {
        set(d);
    }

    /**
     * Set the SqlDateWritable based on the days since epoch date.
     * @param d integer value representing days since epoch date
     */
    public void set(int d) {
        date = Date.ofEpochDay(d);
    }

    /**
     * Set the SqlDateWritable based on the year/month/day of the date in the local timezone.
     * @param d Date value
     */
    public void set(Date d) {
        if (d == null) {
            date = new Date();
            return;
        }

        set(d.toEpochDay());
    }

    public void set(SqlDateWritable d) {
        set(d.getDays());
    }

    /**
     * @return Date value corresponding to the date in the local time zone
     */
    public Date get() {
        return date;
    }

    public int getDays() {
        return date.toEpochDay();
    }

    /**
     *
     * @return time in seconds corresponding to this SqlDateWritable
     */
    public long getTimeInSeconds() {
        return date.toEpochSecond();
    }

    public static Date timeToDate(long seconds) {
        return Date.ofEpochMilli(seconds * 1000);
    }

    public static long daysToMillis(int days) {
        return Date.ofEpochDay(days).toEpochMilli();
    }

    public static int millisToDays(long millis) {
        return Date.ofEpochMilli(millis).toEpochDay();
    }

    public static int dateToDays(Date d) {
        return d.toEpochDay();
    }

    @Deprecated
    public static int dateToDays(java.sql.Date d) {
        return Date.ofEpochMilli(d.getTime()).toEpochDay();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date.setTimeInDays(WritableUtils.readVInt(in));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, (int) date.toEpochDay());
    }

    @Override
    public int compareTo(SqlDateWritable d) {
        return date.compareTo(d.date);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SqlDateWritable)) {
            return false;
        }
        return compareTo((SqlDateWritable) o) == 0;
    }

    @Override
    public String toString() {
        return date.toString();
    }

    @Override
    public int hashCode() {
        return date.hashCode();
    }
}