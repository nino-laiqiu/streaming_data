package demo.datagen;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alibaba.fastjson.JSON;
import marketing.beans.EventBean;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ActionLogGenOne {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "43.142.80.86:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        EventBean logBean = new EventBean();
        logBean.setDeviceId("000053");
        logBean.setEventId("E");
        Map<String, String> ps = new HashMap();
        props.put("p1", "v1");
        logBean.setProperties(ps);
        logBean.setTimeStamp(System.currentTimeMillis());

        String log = JSON.toJSONString(logBean);
        ProducerRecord<String, String> record = new ProducerRecord<>("topic1", log);
        //./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }
}
