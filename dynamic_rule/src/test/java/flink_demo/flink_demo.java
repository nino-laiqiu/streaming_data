package flink_demo;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;

public class flink_demo {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.10.100", 5000);
        socketTextStream.process(new ProcessFunction<String, String>() {
            Counter process_call_count;
            Counter process_call_timeAmount;
            MeterView process_callcnt_persecond;
            @Override
            public void open(Configuration parameters) throws Exception {
                //定义两个度量组
                MetricGroup g1 = getRuntimeContext().getMetricGroup().addGroup("g1");
                MetricGroup g2 = getRuntimeContext().getMetricGroup().addGroup("g2");

                //g1组中定义两个指标
                 process_call_count = g1.counter("process_call_count");
                 process_call_timeAmount = g1.counter("process_call_timeAmount");

                 //g2组中定义两个指标
                 process_callcnt_persecond = g2.meter("process_callcnt_persecond", new MeterView(process_call_timeAmount));

            }

            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                long lStart = System.currentTimeMillis();
                Thread.sleep(RandomUtils.nextInt(300,800));
                long lEnd = System.currentTimeMillis();
                process_call_count.inc();
                process_call_timeAmount.inc(lEnd - lStart);
                process_callcnt_persecond.markEvent();
                collector.collect(s.toUpperCase());
            }
        }).print();

        env.execute();
    }
}
