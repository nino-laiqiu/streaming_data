package demo.router;

import demo.beans.EventParam;
import demo.beans.EventSequenceParam;
import demo.beans.RuleConditions;
import marketing.beans.EventBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import demo.queryservice.ClickHouseQueryService;
import demo.queryservice.HbaseQueryService;
import demo.utils.ConnectionUtils;
import demo.utils.EventParamComparator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleQueryRouter {

    Connection hbaseConnection;
    HbaseQueryService hbaseQueryService;
    java.sql.Connection ckConnection;
    ClickHouseQueryService clickHouseQueryService;
     public SimpleQueryRouter() throws Exception {
         System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.2.0-bin-master");

         hbaseConnection = ConnectionUtils.getHbaseConnection();
         hbaseQueryService = new HbaseQueryService(hbaseConnection);
         //获取ck的连接
         ckConnection = ConnectionUtils.getClickhouseConnection();
         //构造一个ck的查询服务
         clickHouseQueryService = new ClickHouseQueryService(ckConnection);
     }

      public boolean ruleMatch(RuleConditions ruleConditions, EventBean event) throws IOException, SQLException {

          //判断当前事件是否是规则定义的触发事件条件
          if (!EventParamComparator.compare(ruleConditions.getTriggerEvent(),event)) return false;
          log.info("规则条件被触发,触发事件为 {},触发时间为 {}",event,System.currentTimeMillis());

          //计算画像条件是否满足
          Map<String, String> profileConditions = ruleConditions.getUserProfileConditions();
          boolean profileConditionsResult = false;
          if (profileConditions != null){
              profileConditionsResult = hbaseQueryService.queryProfileCondition(event.getDeviceId(), profileConditions);
              //如果画像条件是false则退出
              if (!profileConditionsResult) return false;
          }
          log.info("画像条件被触发");

          //行为次数条件是否满足
          List<EventParam> actionCountConditions = ruleConditions.getActionCountConditionsList();
          if (actionCountConditions != null && actionCountConditions.size() > 0) {
              for (EventParam actionCountCondition : actionCountConditions) {
                  //从ck中获取结果和rule进行比较
                  long countCondition = clickHouseQueryService.queryEventCountCondition(event.getDeviceId(), actionCountCondition);
                  //如果查询到一个规则不满足,则计算规则结束
                  if (countCondition < actionCountCondition.getCountThreshHold()) return false;
              }
          }
          log.info("行为次数条件被触发");

          //行为序列条件
          List<EventSequenceParam> sequenceCondition = ruleConditions.getActionSequenceConditionList();
          if (sequenceCondition != null && sequenceCondition.size() >0) {
              for (EventSequenceParam sequenceParam : sequenceCondition) {
                  int maxStep = clickHouseQueryService.queryEventSequenceCondition(event.getDeviceId(), sequenceParam);
                  //存在多个序列,一个序列中有多个事件,如果一个序列不满足如下的条件就退出
                  if (maxStep < sequenceParam.getEventParamList().size()) return false;
              }
          }
          log.info("行为序列条件被触发");

          log.info("规则条件完全匹配,触发事件为 {},触发时间为 {}",event,System.currentTimeMillis());
          return true;
      }
}
