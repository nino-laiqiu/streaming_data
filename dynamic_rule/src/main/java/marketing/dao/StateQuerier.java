package marketing.dao;

import marketing.beans.EventBean;
import marketing.beans.EventCombinationCondition;
import marketing.beans.EventCondition;
import marketing.utils.EventUtil;
import org.apache.flink.api.common.state.ListState;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class StateQuerier {
    ListState<EventBean> eventBeanListState;
   public StateQuerier(ListState<EventBean> eventBeanListState){
         this.eventBeanListState = eventBeanListState;
   }
    public String getEventCombinationConditionStr(String deviceId , EventCombinationCondition combinationCondition, long queryRangeStart , long queryRangeEnd) throws Exception {
        //获取状态state中的数据迭代器
        //todo 这里只能get,不能上面传入迭代器,否则迭代一次,就不能迭代第二次了
        Iterable<EventBean> eventBean = eventBeanListState.get();
        //获取事件组合条件中的感兴趣的事件
        List<EventCondition> eventConditionList = combinationCondition.getEventConditionList();
        StringBuilder sb = new StringBuilder();
        for (EventBean bean : eventBean) {
            if (bean.getTimeStamp() > queryRangeStart && bean.getTimeStamp() < queryRangeEnd && deviceId.equals(bean.getDeviceId())){
                //判断当前迭代到的bean,是否是条件中感兴趣的事件
                for (int i = 1; i < eventConditionList.size(); i++) {
                    //条件中感兴趣的事件与bean进行对比
                   if (EventUtil.eventMatchCondition(bean,eventConditionList.get(i-1)) ) {
                       sb.append(i);
                       //只要匹配到就结束
                       break;
                   }
                }

            }
        }

  return sb.toString();
    }



    public int  queryEventCombinationConditionStr(String deviceId , EventCombinationCondition combinationCondition, long queryRangeStart , long queryRangeEnd) throws Exception {
        //获取匹配到的事件
        String conditionStr = getEventCombinationConditionStr(deviceId, combinationCondition, queryRangeStart, queryRangeEnd);
        //封装正则匹配的逻辑
        int cnt = EventUtil.sequenceStrMatchRegexCount(conditionStr, combinationCondition.getMatchPattern());
        //匹配到的步骤数
        return cnt;


    }
}
