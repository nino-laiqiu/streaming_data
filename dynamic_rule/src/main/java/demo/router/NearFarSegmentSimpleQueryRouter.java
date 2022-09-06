package demo.router;

import demo.beans.EventParam;
import demo.beans.EventSequenceParam;
import demo.beans.RuleConditions;
import marketing.beans.EventBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.hadoop.hbase.client.Connection;
import demo.queryservice.ClickHouseQueryService;
import demo.queryservice.HbaseQueryService;
import demo.queryservice.StateQueryService;
import demo.utils.ConnectionUtils;
import demo.utils.EventParamComparator;
import demo.utils.SegmentQueryUtil;

import java.util.List;
import java.util.Map;

@Slf4j
public class NearFarSegmentSimpleQueryRouter {

    HbaseQueryService hbaseQueryService;
    java.sql.Connection ckConnection;
    ClickHouseQueryService clickHouseQueryService;
    StateQueryService stateQueryService;

    public NearFarSegmentSimpleQueryRouter(ListState<EventBean> listState) throws Exception {

        System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.2.0-bin-master");
        Connection hbaseConnection = ConnectionUtils.getHbaseConnection();

        hbaseQueryService = new HbaseQueryService(hbaseConnection);
        //获取ck的连接
        ckConnection = ConnectionUtils.getClickhouseConnection();
        //构造一个ck的查询服务
        clickHouseQueryService = new ClickHouseQueryService(ckConnection);

        //构造一个state查询服务
        stateQueryService = new StateQueryService(listState);


    }

    public boolean ruleMatch(RuleConditions ruleConditions, EventBean event, ListState<EventBean> eventBeanListState) throws Exception {

        //判断当前事件是否是规则定义的触发事件条件
        if (!EventParamComparator.compare(ruleConditions.getTriggerEvent(), event)) return false;

        log.info("规则条件被触发,触发事件为 {},触发时间为 {}", event, System.currentTimeMillis());

        //计算画像条件是否满足
        Map<String, String> profileConditions = ruleConditions.getUserProfileConditions();
        boolean profileConditionsResult = false;
        if (profileConditions != null) {
            profileConditionsResult = hbaseQueryService.queryProfileCondition(event.getDeviceId(), profileConditions);
            //如果画像条件是false则退出
            if (!profileConditionsResult) return false;
        }
        log.info("画像条件被触发");
        //获取当前时间对应查询分界点
        long segmentPoint = SegmentQueryUtil.getSegmentPoint(event.getTimeStamp());

        //行为次数条件是否满足
        List<EventParam> actionCountConditions = ruleConditions.getActionCountConditionsList();
        if (actionCountConditions != null && actionCountConditions.size() > 0) {
            for (EventParam actionCountCondition : actionCountConditions) {
                //判断条件中的时间跨度,落在分界点的左边?右边?跨界?
                //注意比较对象是什么?
                //分界点左边(只查clickhouse)
                if (actionCountCondition.getTimeRangeEnd() < segmentPoint) {
                    //直接读取ck数据
                    long countCondition = clickHouseQueryService.queryEventCountCondition(event.getDeviceId(), actionCountCondition);
                    if (countCondition < actionCountCondition.getCountThreshHold()) return false;
                }

                //分界点右边(只查flink state)
                else if (actionCountCondition.getTimeRangeStart() > segmentPoint) {
                    //读取flink状态的数据
                    long cnt = stateQueryService.stateQueryEventCount(event, eventBeanListState, actionCountCondition, actionCountCondition.getTimeRangeStart(), actionCountCondition.getTimeRangeEnd());
                    if (cnt < actionCountCondition.getCountThreshHold()) return false;
                } else {
                    // 跨界查询,先查状态,接着查ck
                    //分界点 --> 条件时间结束点
                    long cntInState = stateQueryService.stateQueryEventCount(event, eventBeanListState, actionCountCondition, segmentPoint, actionCountCondition.getTimeRangeEnd());
                    if (cntInState < actionCountCondition.getCountThreshHold()) {
                        //条件时间开始点 --> 分界点
                        log.info("cntInState"+cntInState);
                        long cntInCk = clickHouseQueryService.queryEventCountCondition(event.getDeviceId(), actionCountCondition, actionCountCondition.getTimeRangeStart(), segmentPoint);
                        if (cntInState + cntInCk < actionCountCondition.getCountThreshHold()) return false;
                    }
                }

            }
        }
        log.info("行为次数条件被触发");

        //行为序列条件
        List<EventSequenceParam> sequenceCondition = ruleConditions.getActionSequenceConditionList();
        if (sequenceCondition != null && sequenceCondition.size() > 0) {
            for (EventSequenceParam sequenceParam : sequenceCondition) {
                //分界点左边(只查clickhouse)
                if (sequenceParam.getTimeRangeEnd() < segmentPoint) {
                    int stepAllInCk = clickHouseQueryService.queryEventSequenceCondition(event.getDeviceId(), sequenceParam);
                    //存在多个序列,一个序列中有多个事件,如果一个序列不满足如下的条件就退出
                    if (stepAllInCk < sequenceParam.getEventParamList().size()) return false;
                }
                //分界点右边(只查flink state)
                else if (sequenceParam.getTimeRangeStart() > segmentPoint) {
                    //todo 从state中查询指定事件序列的最大匹配步骤
                    int stepAllInState = stateQueryService.QueryEventSequence(sequenceParam.getEventParamList(), sequenceParam.getTimeRangeStart(), sequenceParam.getTimeRangeEnd());
                    if (stepAllInState < sequenceParam.getEventParamList().size()) return false;
                }
                // 跨界查询,先查ck,接着查状态
                else {
                    // todo 这里为什么先查ck???? 行为事件是有序的
                    int stepInCk = clickHouseQueryService.queryEventSequenceCondition(event.getDeviceId(), sequenceParam, sequenceParam.getTimeRangeStart(), segmentPoint);
                    //条件时间开始点 --> 分界点
                    log.info("stepInCk"+stepInCk);
                    if (stepInCk  < sequenceParam.getEventParamList().size()) {
                        //截取序列,根据ck中最大匹配步骤数,来修减条件中的事件序列
                        List<EventParam> interceptEventSequence = sequenceParam.getEventParamList().subList(stepInCk, sequenceParam.getEventParamList().size());
                        int stepInState = stateQueryService.QueryEventSequence(interceptEventSequence,segmentPoint, sequenceParam.getTimeRangeEnd());
                        log.info("stepInState"+stepInState);
                        if (stepInCk + stepInState < sequenceParam.getEventParamList().size()) return false;
                    }
                }
            }
        }
        log.info("行为序列条件被触发");

        log.info("规则条件完全匹配,触发事件为 {},触发时间为 {}", event, System.currentTimeMillis());
        return true;
    }

}
