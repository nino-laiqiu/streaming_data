package demo.beans;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RuleConditions {
    //规则ID
    private String ruleId;
    // 触发条件
    private EventParam triggerEvent;
    //画像属性条件
    private Map<String,String> userProfileConditions;
    //行为次数条件
    private List<EventParam> actionCountConditionsList;
    //行为序列条件
    private List<EventSequenceParam> actionSequenceConditionList;
}
