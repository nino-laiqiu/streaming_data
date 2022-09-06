package marketing.functions;

import demo.beans.RuleConditions;
import demo.beans.RuleMatchResult;
import demo.router.NearFarSegmentSimpleQueryRouter;
import demo.utils.RuleSimulator;
import demo.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import marketing.beans.EventBean;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 规则:
 * 触发事件:K事件,事件属性(p2=v1)
 * 画像属性:tag87=v2,tag26=v1
 * 行为次数条件 2021 -当前 事件C[p6=v8,p12=v5] 做过两次
 */
@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {
    NearFarSegmentSimpleQueryRouter simpleQueryRouter;
    ListState<EventBean> beansState;
    @Override
    public void open(Configuration parameters) throws Exception {
        System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.2.0-bin-master");
        //simpleQueryRouter = new SimpleQueryRouter();
        //下面的顺序.....
        beansState = getRuntimeContext().getListState(StateDescContainer.getEventStateDescriptor());
        simpleQueryRouter = new NearFarSegmentSimpleQueryRouter(beansState);
    }

    @Override
    public void processElement(EventBean event, KeyedProcessFunction<String, EventBean, RuleMatchResult>.Context context, Collector<RuleMatchResult> collector) throws Exception {
        //log.info("收到一条数据"+event.getDeviceId());
        beansState.add(event);
        //获取规则
        RuleConditions ruleConditions = RuleSimulator.getRule();
        boolean ruleMatch = simpleQueryRouter.ruleMatch(ruleConditions, event,beansState);
        if (!ruleMatch) return;


        //todo 随机事件模拟器
        if (RandomUtils.nextInt(1, 10) % 3 == 0) {
            RuleMatchResult matchResult = new RuleMatchResult(event.getDeviceId(), ruleConditions.getRuleId(), event.getTimeStamp(), System.currentTimeMillis());
            collector.collect(matchResult);
        }
    }
}
