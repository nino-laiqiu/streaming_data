package marketing.utils;

import marketing.beans.EventCombinationCondition;
import marketing.beans.EventCondition;
import marketing.beans.MarketingRule;
import com.alibaba.fastjson.JSON;
import java.util.Arrays;
import java.util.HashMap;

public class RuleSimulator {


    public static MarketingRule getRule(){

        MarketingRule rule = new MarketingRule();
        rule.setRuleId("rule_001");
        //keyby的字段
        rule.setKeyByFields("deviceId");

        // 触发事件条件
        HashMap<String, String> map1 = new HashMap<>();
        map1.put("p2","v2");
       // map1.put("p3","v4");
        EventCondition triggerEvent = new EventCondition("K",map1,1000000000000L,2000000000000L,1,999);
        rule.setTriggerEventCondition(triggerEvent);

        // 画像条件
        HashMap<String, String> map2 = new HashMap<>();
        map2.put("tag1","v1");
        map2.put("tag26","v1");
        rule.setUserProfileConditions(map2);



        // 单个行为次数条件列表
        String eventId = "C";
        HashMap<String, String> map3 = new HashMap<>();
        map3.put("p6","v8");
        map3.put("p12","v5");
        // 生成规则次数条件附带的ck查询sql
        long startTime = 1000000000000L;
        long endTime = 2000000000000L;
        String sql1 = "" +
                "SELECT \n" +
                "eventId \n" +
                "from userprofile_detail\n" +
                "where eventId='C' \n" +
                "and deviceId=? and timeStamp between ? and ? ";
        String rPattern1 = "(1)";

        EventCondition e = new EventCondition(eventId,map3,startTime,endTime,1,999);
        EventCombinationCondition eventGroupParam = new EventCombinationCondition(startTime, endTime, 1,999, Arrays.asList(e),rPattern1,"ck",sql1,"001");

        // 行为组合条件
        long st = 1000000000000L;
        long ed = 2000000000000L;

        String eventId1 = "A";
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("p3","v2");
        m1.put("p2","v2");
        EventCondition e1 = new EventCondition(eventId1,m1,st,ed,1,999);

        String eventId2 = "C";
        HashMap<String, String> m2 = new HashMap<>();
        m2.put("p1","v1");
        m2.put("p3","v1");
        EventCondition e2 = new EventCondition(eventId2,m2,st,ed,1,999);

        String eventId3 = "F";
        HashMap<String, String> m3 = new HashMap<>();
        m3.put("p1","v1");
        m3.put("p4","v1");
        EventCondition e3 = new EventCondition(eventId3, m3,  st, ed, 1,999);

        String sql2 =
                "select                                    \n" +
                        "eventId                                   \n" +
                        "from userprofile_detail                         \n" +
                        "where deviceId = ?                        \n" +
                        "and timeStamp between ? and ?             \n" +
                        "and   (                                   \n" +
                        " (eventId='A' )  \n" +
                        " or                                       \n" +
                        " (eventId='C' )  \n" +
                        " or                                       \n" +
                        " (eventId='F' )" +
                        " )" ;
        String rPattern2 = "(1.*2.*3)";
        EventCombinationCondition eventGroupParam2 = new EventCombinationCondition(st,ed,1,999,Arrays.asList(e1,e2,e3),rPattern2,"ck",sql2,"002");

        rule.setEventCombinationConditionList(Arrays.asList(eventGroupParam,eventGroupParam2));

        return rule;
    }

    public static void main(String[] args) {
        MarketingRule rule = getRule();
        String json = JSON.toJSONString(rule);
        System.out.println(json);
    }

}
