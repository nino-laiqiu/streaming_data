package marketing.functions;


import lombok.extern.slf4j.Slf4j;
import marketing.beans.*;
import marketing.controller.TriggerModelRuleMatchController;
import marketing.utils.RuleSimulator;
import marketing.utils.StateDescContainer;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * rule{
 *     触发条件
 *     画像条件
 *     行为组合条件
 *     定时条件{
 *         行为组合条件
 *     }
 * }
 */
@Slf4j
public class RuleMatchKeyedProcessFunction_v1 extends KeyedProcessFunction<String, EventBean, RuleMatchResult>  {
    ListState<EventBean> listState;
    ListState<Tuple2<MarketingRule, Long>> ruleTimerState;
    TriggerModelRuleMatchController controller;
    List<MarketingRule> rules;
    @Override
    public void open(Configuration parameters) throws Exception {
        System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.2.0-bin-master");
        listState = getRuntimeContext().getListState(StateDescContainer.getEventBeansDesc());
        ruleTimerState = getRuntimeContext().getListState(StateDescContainer.ruleMapStateDesc());
        controller = new TriggerModelRuleMatchController(this.listState);
        //获取规则
        MarketingRule rule = RuleSimulator.getRule();
        rules = Arrays.asList(rule);
    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, RuleMatchResult>.Context context, Collector<RuleMatchResult> out) throws Exception {

        listState.add(eventBean);
       // log.info("接收一条数据 {}",eventBean);

        for (MarketingRule rule : rules) {
            boolean b =false;
            //判断规则中的分组依据,和当前哦进入的数据分组依据是否相同,相同才进行计算

            /**
             *  if (rule.getKeyByFields().equals(dynamicKeyedBean.getKeyNames())) {
             *                 //规则匹配计算,看规则的所有条件是否满足
             *                  b = controller.ruleMatch(rule, eventBean);
             *             }
             */

            b = controller.ruleMatch(rule, eventBean);
            //如果满足
            if (b) {
                //判断是否是带定时器的规则
                if (rule.isOnTimer()) {
                    //注册定时器
                    List<TimerCondition> timerConditionList = rule.getTimerConditionList();
                    // 简化起见,限定一个规则中只有一个时间条件
                    // todo 其实并不能判断例如是 ab之间间隔3分钟,之后是c事件,实际中解决的是在规定事件内业务规则的全部事件是否满足
                    TimerCondition timerCondition = timerConditionList.get(0);
                    // todo 可能同一个时间
                    context.timerService().registerEventTimeTimer(eventBean.getTimeStamp() + timerCondition.getTimeLate());
                    //在定时信息state中进行记录
                    ruleTimerState.add(Tuple2.of(rule,eventBean.getTimeStamp() +timerCondition.getTimeLate()));
                }
            }
            //不带定时条件
            else {
                out.collect(new RuleMatchResult(eventBean.getEventId(), rule.getRuleId(), eventBean.getTimeStamp(), System.currentTimeMillis()));
            }
        }
    }


    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, EventBean, RuleMatchResult>.OnTimerContext ctx, Collector<RuleMatchResult> out) throws Exception {
        Iterable<Tuple2<MarketingRule, Long>> ruleTimerIterable = ruleTimerState.get();
        Iterator<Tuple2<MarketingRule, Long>> ruleTimerIterator = ruleTimerIterable.iterator();
        while (ruleTimerIterator.hasNext()) {
            Tuple2<MarketingRule, Long> tp = ruleTimerIterator.next();
            //判断这个(规则:定时点),是否对应本次的触发点
              if (tp.f1 == timestamp){
                  //如果对应,检查该规则的定时条件(定时条件中包含的就是行为条件列表)
                  TimerCondition timerCondition = tp.f0.getTimerConditionList().get(0);
                  //调用service去检查在条件指定的时间范围内,事件的组合发生次数是否满足
                  boolean b = controller.isMatchTimeCondition(ctx.getCurrentKey(), timerCondition, timestamp - timerCondition.getTimeLate(), timestamp);
                  //清理已经检查完毕的规则定时点state信息
                  ruleTimerIterator.remove();
                  if (b){
                    out.collect(new RuleMatchResult(ctx.getCurrentKey(),tp.f0.getRuleId(),timestamp,System.currentTimeMillis()));
                  }
              }
              //todo ADD 增加删除过期定时信息的逻辑
              if (tp.f1 < timestamp){
                   ruleTimerIterator.remove();
              }
        }
    }
}
