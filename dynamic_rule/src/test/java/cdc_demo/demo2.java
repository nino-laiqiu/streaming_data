package cdc_demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class demo2 {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<String> streamSource = environment.readTextFile("F:\\project\\java project\\streaming_data\\dynamic_rule\\src\\main\\resources\\application.properties");
        streamSource.print();
        environment.execute();
    }
}
