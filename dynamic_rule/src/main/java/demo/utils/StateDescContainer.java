package demo.utils;


import marketing.beans.EventBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

public class StateDescContainer {

    public static ListStateDescriptor<EventBean> getEventStateDescriptor(){
        ListStateDescriptor<EventBean> eventStateDescriptor = new ListStateDescriptor<EventBean>("event_beans", EventBean.class);
        //设置TTL
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2L)).build();
        eventStateDescriptor.enableTimeToLive(ttlConfig);
        return eventStateDescriptor;


    }

}
