package demo.utils;

import org.apache.commons.lang3.time.DateUtils;
import demo.pojo.DateUtil;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 分段查询的工具
 */
public class SegmentQueryUtil {

    public static long getSegmentPoint(long timeStamp){
        Date date = DateUtils.ceiling(new Date(timeStamp - 2 * 60 * 60 * 1000L), Calendar.HOUR);
        return date.getTime();
    }

    public static void main(String[] args) throws ParseException {
        long segmentPoint = getSegmentPoint(new DateUtil().getTime("2022-08-11 12:00:00"));
        // 2022-08-17 12:00:00 ---> 2022-08-17 11:00:00 ??
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sd = sdf.format(new Date(Long.parseLong(String.valueOf(segmentPoint))));
        System.out.println(sd);
    }
}
