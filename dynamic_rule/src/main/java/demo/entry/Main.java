package demo.entry;

import demo.beans.RuleMatchResult;
import demo.function.Json2BeanMapFunction;
import demo.function.RuleMatchKeyedProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import demo.function.KafkaSourceBuilder;
import marketing.beans.EventBean;

import java.util.Objects;

public class Main {

    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //env.setParallelism(1);

        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        DataStreamSource<String> addSource = env.addSource(kafkaSourceBuilder.build("topic1"));

        SingleOutputStreamOperator<EventBean> map = addSource.map(new Json2BeanMapFunction());
        SingleOutputStreamOperator<EventBean> dsBean = map.filter(Objects::nonNull);

        SingleOutputStreamOperator<RuleMatchResult> ruleMatchResultSingleOutputStreamOperator = dsBean.keyBy(EventBean::getDeviceId).process(new RuleMatchKeyedProcessFunction());
        ruleMatchResultSingleOutputStreamOperator.print();
        env.execute();
    }
}
