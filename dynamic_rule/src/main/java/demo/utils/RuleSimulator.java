package demo.utils;

import demo.beans.EventParam;
import demo.beans.EventSequenceParam;
import demo.beans.RuleConditions;
import demo.pojo.DateUtil;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class RuleSimulator {
    public static RuleConditions getRule() throws ParseException {
        RuleConditions ruleConditions = new RuleConditions();

        ruleConditions.setRuleId("rule-001");

        //触发事件条件
        HashMap<String, String> map1 = new HashMap<>();
        map1.put("p2", "v1");
        EventParam eventParam = new EventParam("K", map1, 0, -1, -1,"");
        ruleConditions.setTriggerEvent(eventParam);

        //画像条件
        HashMap<String, String> map2 = new HashMap<>();
        map2.put("tag87", "v2");
        map2.put("tag26", "v1");
        ruleConditions.setUserProfileConditions(map2);

        //行为次数条件
        HashMap<String, String> map3 = new HashMap<>();
        map3.put("p2", "v2");
        //map3.put("p12", "v5");
        DateUtil dateUtil = new DateUtil();
        long startTime = dateUtil.getTime("1998-04-24 00:00:00");
        long endTime = Long.MAX_VALUE;

        //String sql ="SELECT COUNT(1) AS cnt FROM userprofile_detail WHERE  eventId = 'C' AND properties['p6'] = 'v8' AND properties['p12'] = 'v5' AND deviceId =? AND timeStamp BETWEEN " + startTime + " AND " + endTime;
        //String sql_test ="SELECT COUNT(1) AS cnt FROM userprofile_detail WHERE properties['p2'] = 'v2' AND  deviceId =? AND timeStamp BETWEEN " + startTime + " AND " + endTime;
        String sql_test ="SELECT COUNT(1) AS cnt FROM userprofile_detail WHERE properties['p2'] = 'v2' AND  deviceId =? AND timeStamp BETWEEN ? AND ? " ;
        EventParam eventParam1 = new EventParam("C", map3, 1, dateUtil.getTime("1998-04-24 00:00:00"), Long.MAX_VALUE,sql_test);
        ruleConditions.setActionCountConditionsList(Collections.singletonList(eventParam1));


        //行为序列条件
        long st = dateUtil.getTime("1998-04-24 00:00:00");
        long ed = Long.MAX_VALUE;
        String eventId1 = "A";
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("p3","v2");
        EventParam event1= new EventParam(eventId1, m1, -1, st, ed, "");

        String eventId2 = "C";
        HashMap<String, String> m2 = new HashMap<>();
        m1.put("p1","v1");
        EventParam event2= new EventParam(eventId2, m2, -1, st, ed, "");


        String eventId3 = "F";
        HashMap<String, String> m3 = new HashMap<>();
        m1.put("p1","v1");
        EventParam event3= new EventParam(eventId3, m3, -1, st, ed, "");

        String seqSql = "SELECT \n" +
                "sequenceMatch('(?1).*(?2).*(?3)')(\n" +
                "toDateTime(`timeStamp`),\n" +
                "eventId='A',\n" +
                "eventId='C',\n" +
                "eventId='F'\n" +
                ") AS isMatch3,\n" +
                "sequenceMatch('(?1).*(?2)')(\n" +
                "toDateTime(`timeStamp`),\n" +
                "eventId='A',\n" +
                "eventId='C',\n" +
                "eventId='F'\n" +
                ") AS isMatch2,\n" +
                "sequenceMatch('(?1)')(\n" +
                "toDateTime(`timeStamp`),\n" +
                "eventId='A',\n" +
                "eventId='C',\n" +
                "eventId='F'\n" +
                ") AS isMatch1\n" +
                "FROM \n" +
                "userprofile_detail\n" +
                "WHERE deviceId = ? \n" +
                "AND timeStamp BETWEEN ? AND ? \n" +
                "AND (\n" +
                "eventId = 'A' AND properties['p3'] = 'v2'\n" +
                "OR\n" +
                "eventId = 'C' AND properties['p1'] = 'v1'\n" +
                "OR\n" +
                "eventId = 'F' AND properties['p1'] = 'v1'\n" +
                ")\n" +
                "GROUP BY deviceId";

        EventSequenceParam eventSequenceParam = new EventSequenceParam("rule_001", st, ed, Arrays.asList(event1, event2, event3), seqSql);
        ruleConditions.setActionSequenceConditionList(Collections.singletonList(eventSequenceParam));

        return ruleConditions;
    }
}
