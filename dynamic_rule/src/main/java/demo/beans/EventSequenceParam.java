package demo.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class EventSequenceParam {
    private String ruleId;
    private long timeRangeStart;
    private long timeRangeEnd;
    private List<EventParam> eventParamList;

    private String sequenceQuerySql;
}