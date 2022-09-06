package marketing.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import marketing.beans.EventBean;
import marketing.beans.EventCombinationCondition;
import marketing.beans.TimerCondition;
import marketing.dao.ClickHouseQuerier;
import marketing.dao.HbaseQuerier;
import marketing.dao.StateQuerier;
import marketing.utils.ConfigNames;
import marketing.utils.ConnectionUtils;
import marketing.utils.CrossTimeQueryUtil;
import marketing.utils.EventUtil;
import org.apache.flink.api.common.state.ListState;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class TriggerModeRulelMatchServiceImpl {
    ClickHouseQuerier clickHouseQuery;
    HbaseQuerier hbaseQuery;
    StateQuerier stateQuery;

    public TriggerModeRulelMatchServiceImpl(ListState<EventBean> listState) throws Exception {
        Config config = ConfigFactory.load();
        String profileTableName = config.getString(ConfigNames.HBASE_PROFILE_TABLE);
        String profileFamily = config.getString(ConfigNames.HBASE_PROFILE_FAMILY);
        Connection clickhouseConn = ConnectionUtils.getClickhouseConnection();
        clickHouseQuery = new ClickHouseQuerier(clickhouseConn);

        org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionUtils.getHbaseConnection();
        hbaseQuery = new HbaseQuerier(hbaseConn, profileTableName,profileFamily );

        stateQuery = new StateQuerier(listState);
    }
    //画像条件匹配
    public boolean matchProfileCondition(Map<String,String> profileCondition, String deviceId) throws IOException {
        return hbaseQuery.queryProfileConditionIsMatch(profileCondition,deviceId);

    }

    //行为组合匹配(计算一个行为条件)
    public boolean matchEventCombinationCondition(EventCombinationCondition combinationCondition , EventBean eventBean) throws Exception {
        //获取当前事件时间对应的查询分界点
        long segmentPoint = CrossTimeQueryUtil.getSegmentPoint(eventBean.getTimeStamp());
        //判断条件的时间区间是否跨节点
        long conditionStart = combinationCondition.getTimeRangeStart();
        long conditionEnd = combinationCondition.getTimeRangeEnd();
        //查状态
        if (conditionStart > segmentPoint){
            int step = stateQuery.queryEventCombinationConditionStr(eventBean.getDeviceId(), combinationCondition, conditionStart, conditionEnd);
            return  step >= combinationCondition.getMinLimit() && step <= combinationCondition.getMaxLimit();

        }
        //查ck
        else if (conditionEnd < segmentPoint){
            int step = clickHouseQuery.queryEventCombinationConditionCount(eventBean.getDeviceId(), combinationCondition, conditionStart, conditionEnd);
            return  step >= combinationCondition.getMinLimit() && step <= combinationCondition.getMaxLimit();
        }
        //跨界查询
        else {
             //先查state,不满足则查ck
             // todo 要注意一下的是,查state是要判断是否在条件时间区间的,因为是对beans的遍历,但是查ck是不用的直接查业务条件的时间范围的
            int step = stateQuery.queryEventCombinationConditionStr(eventBean.getDeviceId(), combinationCondition, segmentPoint, conditionEnd);
            if (step >= combinationCondition.getMinLimit()) return true;


            //先从ck中查询满足条件的事件序列字符串,拼接,state中查询到满足条件的事件序列字符串,然后作为整体匹配正则表达式
            String str1 = clickHouseQuery.getEventCombinationConditionStr(eventBean.getDeviceId(), combinationCondition, conditionStart, segmentPoint);
            String str2 = stateQuery.getEventCombinationConditionStr(eventBean.getDeviceId(), combinationCondition, segmentPoint, conditionEnd);
            step = EventUtil.sequenceStrMatchRegexCount(str1 + str2, combinationCondition.getMatchPattern());
            return  step >= combinationCondition.getMinLimit() && step <= combinationCondition.getMaxLimit();
        }
    }


}
