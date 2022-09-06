package PratterDemo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class demo1 {

    public static void main(String[] args) {
          String st = "abcddddf";
          String pattern = "ab.*f";
        Pattern compile = Pattern.compile(pattern);
        Matcher matcher = compile.matcher(st);
        int cnt = 0;
        while (matcher.find()) {
            cnt ++;
        }
        System.out.println(cnt);
    }
}
