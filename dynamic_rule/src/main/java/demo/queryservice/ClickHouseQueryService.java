package demo.queryservice;

import demo.beans.EventParam;
import demo.beans.EventSequenceParam;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class ClickHouseQueryService implements QueryService{
     Connection ckConn;
     public ClickHouseQueryService(Connection ckConn){
         this.ckConn =ckConn;
     }

     public long queryEventCountCondition(String deviceId, EventParam eventParam) throws SQLException {
          return queryEventCountCondition(deviceId,eventParam,eventParam.getTimeRangeStart(),eventParam.getTimeRangeEnd());
     }


    public long  queryEventCountCondition(String deviceId, EventParam eventParam, long timeRangeStart, long timeRangeEnd) throws SQLException {
        log.info("收到一个ck查询请求: 参数为 deviceId ={},eventParam={},deviceId ={} ,timeRangeStart ={},timeRangeEnd ={}",deviceId,eventParam,deviceId,timeRangeStart,timeRangeEnd);
        //String sql ="SELECT COUNT(1) AS cnt FROM userprofile_detail WHERE  eventId = 'C' AND properties['p2'] = 'v2' AND properties['p12'] = 'v5' AND deviceId =? AND timeStamp BETWEEN ? AND ? "  ;
        //String sql_test ="SELECT COUNT(1) AS cnt FROM userprofile_detail WHERE  eventId = 'C'  AND deviceId =? AND timeStamp BETWEEN ? AND ? "  ;
        PreparedStatement prepareStatement = ckConn.prepareStatement(eventParam.getQuerySql());
        prepareStatement.setString(1,deviceId);
        prepareStatement.setLong(2,timeRangeStart);
        prepareStatement.setLong(3,timeRangeEnd);
         /*prepareStatement.setLong(2,eventParam.getTimeRangeStart());
         prepareStatement.setLong(3,eventParam.getTimeRangeEnd());*/
        ResultSet resultSets = prepareStatement.executeQuery();
        long result = 0;
        while (resultSets.next()) {
            result = resultSets.getLong("cnt");
        }
        log.info("queryEventCountCondition {}",result);
        return result;
    }


    public int queryEventSequenceCondition(String deviceId, EventSequenceParam eventSequenceParam) throws SQLException {
        return queryEventSequenceCondition(deviceId,eventSequenceParam,eventSequenceParam.getTimeRangeStart(),eventSequenceParam.getTimeRangeEnd());
    }

     public int queryEventSequenceCondition(String deviceId , EventSequenceParam eventSequenceParam, long timeRangeStart, long timeRangeEnd) throws SQLException {


         PreparedStatement preparedStatement = ckConn.prepareStatement(eventSequenceParam.getSequenceQuerySql());
         preparedStatement.setString(1,deviceId);
         preparedStatement.setLong(2,timeRangeStart);
         preparedStatement.setLong(3,timeRangeEnd);
         ResultSet resultSets = preparedStatement.executeQuery();

         /**
          * isMatch3,isMatch2,isMatch1
          * 1,1,1
          */

         /**
          * 这里不知道有多少个条件,考虑从EventSequenceParam中获取
          */
         int step = 0;
         //todo resultSets.next()没有判断是否为true
         if (resultSets.next()) {
             for (int i = 1; i <= eventSequenceParam.getEventParamList().size(); i++) {
                 int resultSetsInt = resultSets.getInt(i);
                 //从左边取查询结果中的字段,如果取到的字段为1,说明这个步骤配配备了,且这时最大匹配,根据i和最大匹配的关系,得到最大的步骤数
                 if (resultSetsInt == 1) {
                     step = eventSequenceParam.getEventParamList().size() - (i - 1);
                     break;
                 }
             }
             return step;
         }
         return step;
     }
}
