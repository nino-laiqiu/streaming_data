package demo.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class EventParam {
    private String eventId;
    private Map<String,String> eventProperties;
    private int countThreshHold;
    private long timeRangeStart;
    private long timeRangeEnd;

    //事件次数规则条件对应的查询SQL
    private String querySql;
}
