package com.alibaba.datax.plugin.reader.kafkareader;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间日期工具类
 */
public class DateUtil {
    public static String format = "yyyy-MM-dd HH:mm:ss";
    public static SimpleDateFormat sdf = new SimpleDateFormat(format);

    /**
     * 将当前的时间减去5分钟
     *
     * @param date
     * @return
     */
    public static Date subtraction10Minute(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MINUTE, -10);
        return c.getTime();
    }

    /**
     * 讲date转换成想要的格式
     *
     * @param date
     * @return
     */
    public static String targetFormat(Date date) {
        return sdf.format(date);
    }

    public static Date subtractionMinute(Date date, int count) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MINUTE, -count);
        return c.getTime();
    }

    public static String targetFormat(Date date, String targetStr) {
        SimpleDateFormat sdff = new SimpleDateFormat(targetStr);
        return sdff.format(date);
    }

    public static Date subtraction1Sesond(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.SECOND, -1);
        return c.getTime();
    }

    public static Date subtraction1Day(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DAY_OF_MONTH, -1);
        return c.getTime();
    }

    public static Date subtractionDay(Date date,int count) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DAY_OF_MONTH, -count);
        return c.getTime();
    }

    public static Date add10Minute(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MINUTE, 10);
        return c.getTime();
    }

    /**
     * 给一个日期增加时间，可以传入负数。那就可以减少时间。
     *
     * @param date
     * @param count
     * @return
     */
    public static Date addMinute(Date date, int count) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MINUTE, count);
        return c.getTime();
    }


    public static Date strToDate(String strDate) {
        try {
            return sdf.parse(strDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Long subDate(String date1, String date2, String format) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = sdf.parse(date1);
        Date date3 = sdf.parse(date2);
        return (date3.getTime() - date.getTime()) / 1000;
    }

    public static void main(String[] args) throws ParseException {
        Date date = new Date();
        date = subtractionMinute(date,-60*13+3);
        System.out.println(DateUtil.targetFormat(date).split(" ")[1].substring(0,2));
    }
}
