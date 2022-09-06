package date_demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Date_demo1 {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = "2021-06-18 00:00:00";
        Date date = format.parse(time);//转换成格林威治时间
        Long timeLongNum = date.getTime();//转换为时间戳
        System.out.println(timeLongNum);
    }
}
