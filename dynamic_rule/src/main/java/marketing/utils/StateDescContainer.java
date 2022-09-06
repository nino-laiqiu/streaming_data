package marketing.utils;

import com.sun.javafx.collections.MapListenerHelper;
import marketing.beans.EventBean;
import marketing.beans.MarketingRule;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

public class StateDescContainer {

    /**
     * 近期行为事件存储状态描述器
     */
    public static ListStateDescriptor<EventBean> getEventBeansDesc() {
        ListStateDescriptor<EventBean> eventBeansDesc = new ListStateDescriptor<>("event_beans", EventBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        eventBeansDesc.enableTimeToLive(ttlConfig);

        return eventBeansDesc;
    }

    /**
     * 存储规则和七对应定时器时间
     */

    public static ListStateDescriptor<Tuple2<MarketingRule,Long>> ruleMapStateDesc(){

        ListStateDescriptor<Tuple2<MarketingRule,Long>> descriptor = new ListStateDescriptor<>("rule_time", TypeInformation.of(new TypeHint<Tuple2<MarketingRule,Long>>() {
        }));
        return descriptor;

    }
}
