package demo.pojo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    SimpleDateFormat format;
    public DateUtil() {
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }
    public  Long getTime(String time) throws ParseException {
        Date date = format.parse(time);//转换成格林威治时间
        return date.getTime();//转换为时间戳
    }
}
