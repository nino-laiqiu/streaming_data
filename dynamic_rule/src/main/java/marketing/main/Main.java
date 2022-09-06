package marketing.main;



import demo.function.RuleMatchKeyedProcessFunction;
import marketing.beans.EventBean;
import marketing.beans.RuleMatchResult;
import marketing.functions.Json2BeanMapFunction;
import marketing.functions.KafkaSourceBuilder;
import marketing.functions.RuleMatchKeyedProcessFunction_v1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class Main {

    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //env.setParallelism(1);

        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        DataStreamSource<String> addSource = env.addSource(kafkaSourceBuilder.build());

        SingleOutputStreamOperator<EventBean> map = addSource.map(new Json2BeanMapFunction());
        SingleOutputStreamOperator<EventBean> dsBean = map.filter(Objects::nonNull);


        SingleOutputStreamOperator<RuleMatchResult> ruleMatchResultSingleOutputStreamOperator = dsBean.keyBy(EventBean::getDeviceId).process(new RuleMatchKeyedProcessFunction_v1());
        ruleMatchResultSingleOutputStreamOperator.print();
        env.execute();
    }
}
