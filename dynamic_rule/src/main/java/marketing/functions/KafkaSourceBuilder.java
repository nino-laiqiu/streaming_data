package marketing.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import marketing.utils.ConfigNames;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceBuilder {

    Config config ;

    public KafkaSourceBuilder(){
        config = ConfigFactory.load();
    }

    public <T>FlinkKafkaConsumer<T> build() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.getString(ConfigNames.KAFKA_BOOTSTRAP_SERVERS));
        properties.setProperty("auto.offset.reset", config.getString(ConfigNames.KAFKA_AUTO_OFFSET_RESET));
        String topic = config.getString(ConfigNames.KAFKA_ACTION_DETAIL_TOPIC);
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(topic, (DeserializationSchema<T>) new SimpleStringSchema(), properties);
        return  kafkaConsumer;
    }
}
