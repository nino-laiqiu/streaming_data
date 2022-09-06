package demo.queryservice;

import demo.beans.EventParam;
import marketing.beans.EventBean;
import org.apache.flink.api.common.state.ListState;
import demo.utils.EventParamComparator;

import java.util.List;

public class StateQueryService {
    //todo 通过构造方法传入state,会导致state无数据的问题,解决 RuleMatchKeyedProcessFunction中初始化顺序导致的问题......
    ListState<EventBean> listState;
    public StateQueryService(ListState<EventBean> listState ){
        this.listState = listState;
    }
    //对单个序列(由多个事件组成)条件的判断,一条业务规则中可能有多个序列条件
    public int QueryEventSequence(List<EventParam> eventParamList, long timeRangeStart, long timeRangeEnd) throws Exception {

        //匹配事件的计数器
        int i = 0;
        //匹配到第几步的计数器
        int cnt = 0;
        int log = 0;
        //todo 为什么一直是空的哇??? -->由上
        if (listState != null) {
            Iterable<EventBean> logBeans = listState.get();
            for (EventBean event : logBeans) {
                log ++;
                System.out.println(event + "-----");
                //时间范围要在业务规则内
                if (event.getTimeStamp() >= timeRangeStart && event.getTimeStamp() <= timeRangeEnd && EventParamComparator.compare(eventParamList.get(i), event)) {
                    i++;
                    cnt++;
                    if (i == eventParamList.size()) return cnt;
                }
            }
        }
        System.out.println("QueryEventSequence" + log);
        return cnt;
    }

    public long stateQueryEventCount(EventBean event, ListState<EventBean> eventBeanListState, EventParam actionCountCondition, long queryStart, long queryEnd) throws Exception {
        long cnt = 0;
        Iterable<EventBean> logBeanIterable = eventBeanListState.get();

        for (EventBean logBean : logBeanIterable) {
            System.out.println(logBean);
            //判断遍历到的事件是否落在规则条件时间区间内
            if (event.getTimeStamp() >= queryStart && event.getTimeStamp() <= queryEnd) {
                /**
                 * 挺绕的,ruleConditions中装载的是业务要求的比较规则或是对象上面的if判断是过滤出满足业务要求时间区间内的事件,
                 * 接着的,下面compare是对EventProperties的比较
                 */
                if (EventParamComparator.compare(actionCountCondition, logBean)) cnt++;
            }

        }

        return cnt;
    }
}
