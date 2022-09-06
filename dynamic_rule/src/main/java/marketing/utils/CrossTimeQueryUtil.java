package marketing.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

public class CrossTimeQueryUtil {

    public static long getSegmentPoint(long timeStamp){

        // 给定时间向上取整，倒退2小时
        // 给定时间倒退2小时，然后向上取整
        Date dt = DateUtils.ceiling(new Date(timeStamp - 2 * 60 * 60 * 1000), Calendar.HOUR);

        return dt.getTime();
    }



}
