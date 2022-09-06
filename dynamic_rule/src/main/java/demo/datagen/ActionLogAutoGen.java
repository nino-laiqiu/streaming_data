package demo.datagen;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import marketing.beans.EventBean;

import java.util.HashMap;
import java.util.Properties;

public class ActionLogAutoGen {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "43.142.80.86:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 创建多个线程，并行执行
        genBatch(props);
    }

    private static void genBatch(Properties props) {
        for(int i=0;i<40;i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    // 构造一个kafka生产者客户端
                    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
                    while (true) {

                        EventBean logBean = getLogBean();
                        // 将日志对象，转成JSON
                        String log = JSON.toJSONString(logBean);
                        ProducerRecord<String, String> record = new ProducerRecord<>("topic1", log);
                        kafkaProducer.send(record);
                        try {
                            Thread.sleep(RandomUtils.nextInt(5, 6));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }).start();
        }
    }
    public static EventBean getLogBean(){
        EventBean logBean = new EventBean();
        // 生成的账号形如： 004078
        String account = StringUtils.leftPad(RandomUtils.nextInt(1, 10000) + "", 6, "0");
        logBean.setAccount(account);
        logBean.setAppId("cn.do.log");
        logBean.setLatitude(RandomUtils.nextDouble(10.0, 52.0));
        logBean.setLongitude(RandomUtils.nextDouble(120.0, 160.0));
        logBean.setDeviceType("mi6");
        logBean.setNetType("5G");
        logBean.setOsName("android");
        logBean.setOsVersion("7.5");
        logBean.setReleaseChannel("小米应用市场");
        logBean.setResolution("2048*1024");

        logBean.setAppVersion("2.5");
        logBean.setCarrier("中国移动");
        // deviceid直接用account
        logBean.setDeviceId(account);
        logBean.setIp("10.102.36.88");
        /**
         * 生成事件ID
         */
        logBean.setEventId(RandomStringUtils.randomAlphabetic(1).toUpperCase());

        HashMap<String, String> properties = new HashMap<String, String>();
        for (int i = 0; i < RandomUtils.nextInt(1, 5); i++) {
            // 生成的属性形如：  p1=v1, p2=v1, p3=v2,p4=v1,..... p10=
            properties.put("p" + RandomUtils.nextInt(1, 11), "v" + RandomUtils.nextInt(1, 3));
        }
        logBean.setProperties(properties);
        logBean.setTimeStamp(System.currentTimeMillis());
        logBean.setSessionId(RandomStringUtils.randomNumeric(10, 10));

        return logBean;
    }
}
