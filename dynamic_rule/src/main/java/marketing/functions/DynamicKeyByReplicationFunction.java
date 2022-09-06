package marketing.functions;

import marketing.beans.DynamicKeyedBean;
import marketing.beans.EventBean;
import marketing.beans.MarketingRule;
import marketing.utils.RuleSimulator;
import marketing.utils.RuleSimulatorFromJson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;

public class DynamicKeyByReplicationFunction extends KeyedProcessFunction<String, EventBean, DynamicKeyedBean> {


    HashSet<String> set;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取系统中所有的规则列表
        List<MarketingRule> ruleList = RuleSimulatorFromJson.getRuleList();
        //从规则中遍历每个规则,获取每个规则的keyby字段名,并放入set集合中
        set = new HashSet<>();
        for (MarketingRule rule : ruleList) {
            set.add(rule.getKeyByFields());
        }
    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, DynamicKeyedBean>.Context context, Collector<DynamicKeyedBean> collector) throws Exception {
        //set中是去重后的业务方规定的规则
        for (String keyByFields : set) {
            StringBuilder sb = new StringBuilder();
            String[] fieldNames = keyByFields.split(",");
            for (String fieldName : fieldNames) {
                //java反射获取类型对应的具体值
                Class<?> name = Class.forName("marketing.beans.EventBean");
                Field field = name.getDeclaredField(fieldName);
                //设置权限
                field.setAccessible(true);
                String fieldValue = (String)field.get(eventBean);
                sb.append(fieldValue).append(",");

            }
            String keyByValue = sb.toString().substring(0, sb.length() - 1);
            eventBean.setKeyByValue(keyByValue);
            //每种分组规则都要进行分发,相当于数据扩大了N倍
            collector.collect(new DynamicKeyedBean(keyByValue,keyByFields,eventBean));
        }
    }


}
