package watermark_demo;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * 数据格式 1,A,1661141904107
 */

@Slf4j
public class demo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> socketStream = env.socketTextStream("192.168.10.100", 5666);
        SingleOutputStreamOperator<String> streamOperator = socketStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String event, long l) {
                return Long.parseLong(event.split(",")[2]);
            }
        }));

        KeyedStream<String, String> keyedStream = streamOperator.keyBy(e -> e.split(",")[0]);

        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, String, String>() {
            ListState<Tuple2<String, Long>> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("tmp", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })));
            }

            @Override
            public void processElement(String s, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                String[] arr = s.split(",");
                if ("A".equals(arr[1])) {
                    //todo 一个key只有一个定时器???
                    context.timerService().registerEventTimeTimer(Long.parseLong(arr[2]) + 3000);
                    log.info("注册一个定时器 {}",Long.parseLong(arr[2]));

                }
                if ("C".equals(arr[1])) {
                    listState.add(Tuple2.of("C", Long.parseLong(arr[2])));

                }
            }


            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                log.info("触发一个定时器 {}",timestamp);
                boolean flag = true;
                Iterator<Tuple2<String, Long>> iterator = listState.get().iterator();
                while (iterator.hasNext()) {
                    Tuple2<String, Long> tuple2 = iterator.next();
                    if (tuple2.f1 < timestamp) {
                        //如果发现3秒内有C,则设置flag为false
                        flag = false;
                        iterator.remove();
                    }
                }
                if (flag) out.collect(ctx.getCurrentKey() + "  " +timestamp);
            }
        });

        process.print();
        env.execute();
    }
}

