package jsoup_demo;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class demo {


    public static void main(String[] args) throws IOException {
        //执行计划的任务放在TimerTask的子类中，由Timer进行该任务。
        Timer timer = new Timer();

        //创建一个任务，1s后开始执行，并此后每隔2分钟执行一次。
        timer.schedule(new TimerTask() {
            //创建n，看看循环的次数
            int n=1;
            //创建m，看看循环的次数
            int m=0;
            @Override
            public void run() {
                List<Object> list = new ArrayList<Object>();
                //循环的次数看你博客有多少页就行，也可以爬下来，我懒的爬，直接就写死了，我的博客总共9页。
                    Document doc;
                    try {
                        //先把博客上面所有的链接获取到放在list中
                        doc = Jsoup.connect("https://cloud.tencent.com/developer/article/2093426")
                                .header("Accept-Encoding", "gzip, deflate")
                                .userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0")
                                .maxBodySize(0)
                                .timeout(600000)
                                .get();

                        Elements csdndoc=doc.select("h4");
                        Elements csdnurl =csdndoc.select("a");
                        for (Element element : csdnurl) {
                            String fangwenliang=element.attr("href");
                            list.add(fangwenliang);

                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }

                //让list中的链接循环进行访问，但是因为list有一个广告链接一直存在，所以要进行判断。
                for (int j = 0; j < list.size(); j++) {
                    String url = list.get(j).toString();
                    if (url.equals("https://cloud.tencent.com/developer/article/2093426")) {
                        System.out.println("成功点击博客");
                        try {
                            //开始进行访问，没访问一次就是一次点击。
                            Document shuaxin = Jsoup.connect(url)
                                    .header("Accept-Encoding", "gzip, deflate")
                                    .userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0")
                                    .maxBodySize(0)
                                    .timeout(600000)
                                    .get();
                            m++;
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
                System.out.println("访问博客结束");
                System.out.println("成功点击了博客："+m+"篇");
                Date date = new Date();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println(df.format(date)+ ":正在执行第"+n+"遍");
                n++;
            }
        },1000,120000);//  因为csdn对频繁刷新有限制，所以设置从第一秒开始，每两分钟执行一次
    }
}
