package marketing.controller;

import lombok.extern.slf4j.Slf4j;
import marketing.beans.*;
import marketing.service.TriggerModeRulelMatchServiceImpl;
import marketing.utils.EventUtil;
import org.apache.flink.api.common.state.ListState;

import java.io.IOException;
import java.util.List;
import java.util.Map;
@Slf4j
public class TriggerModelRuleMatchController {
    TriggerModeRulelMatchServiceImpl matchController;
    public TriggerModelRuleMatchController(ListState<EventBean> listState) throws Exception {
         matchController = new TriggerModeRulelMatchServiceImpl(listState);
    }

    public boolean ruleMatch(MarketingRule rule ,EventBean eventBean) throws Exception {

        //判断当前数据是否满足规则的触发事件条件
        EventCondition triggerEventCondition = rule.getTriggerEventCondition();
        if (!EventUtil.eventMatchCondition(eventBean,triggerEventCondition)) return false;

        log.info("触发事件条件满足");
        //判断规则中是否有画像条件
        Map<String, String> userProfileConditions = rule.getUserProfileConditions();
        if (userProfileConditions != null && userProfileConditions.size() > 0){
            if(!matchController.matchProfileCondition(userProfileConditions,eventBean.getDeviceId())) return false;
        }
        log.info("画像条件满足");
        //组合条件的判断
        List<EventCombinationCondition> combinationConditionList = rule.getEventCombinationConditionList();
        if (combinationConditionList != null && combinationConditionList.size() > 0){
            for (EventCombinationCondition conditions : combinationConditionList) {
                //规则定义.规则之间是什么关系  todo 暂时写死
                if (!matchController.matchEventCombinationCondition(conditions,eventBean)) return false;

            }
        }
        log.info("组合条件满足");
        return true;
    }

    //定时器被触发了,在时间范围内查询事件是否满足
    public boolean isMatchTimeCondition(String deviceId, TimerCondition timerCondition , long queryStartTime , long queryEndTime) throws Exception {
        List<EventCombinationCondition> conditionList = timerCondition.getEventCombinationConditionList();
        for (EventCombinationCondition condition : conditionList) {
           condition.setTimeRangeStart(queryStartTime);
           condition.setTimeRangeEnd(queryEndTime);
            EventBean eventBean = new EventBean();

            //todo 需要设置时间戳,后续的查询方法中需要时间戳来计算分界点
            eventBean.setDeviceId(deviceId);
            eventBean.setTimeStamp(queryEndTime);

            boolean b = matchController.matchEventCombinationCondition(condition, eventBean);
            if (!b) return false;
        }
      return true;
    }

}
